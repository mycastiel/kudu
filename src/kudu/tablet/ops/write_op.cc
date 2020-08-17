// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "kudu/tablet/ops/write_op.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <ctime>
#include <new>
#include <ostream>
#include <vector>

#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <google/protobuf/arena.h>

#include "kudu/clock/clock.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/row_operations.h"
#include "kudu/common/schema.h"
#include "kudu/common/timestamp.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/gutil/dynamic_annotations.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/walltime.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/tablet/lock_manager.h"
#include "kudu/tablet/mvcc.h"
#include "kudu/tablet/row_op.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/tablet/tablet_metrics.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/metrics.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/rw_semaphore.h"
#include "kudu/util/trace.h"

DEFINE_int32(tablet_inject_latency_on_apply_write_txn_ms, 0,
             "How much latency to inject when a write op is applied. "
             "For testing only!");
TAG_FLAG(tablet_inject_latency_on_apply_write_txn_ms, unsafe);
TAG_FLAG(tablet_inject_latency_on_apply_write_txn_ms, runtime);

using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tablet {

using pb_util::SecureShortDebugString;
using consensus::CommitMsg;
using consensus::DriverType;
using consensus::ReplicateMsg;
using consensus::WRITE_OP;
using tserver::TabletServerErrorPB;
using tserver::WriteRequestPB;
using tserver::WriteResponsePB;

string WritePrivilegeToString(const WritePrivilegeType& type) {
  switch (type) {
    case WritePrivilegeType::INSERT:
      return "INSERT";
    case WritePrivilegeType::UPDATE:
      return "UPDATE";
    case WritePrivilegeType::DELETE:
      return "DELETE";
  }
  LOG(DFATAL) << "not reachable";
  return "";
}

void AddWritePrivilegesForRowOperations(const RowOperationsPB::Type& op_type,
                                        WritePrivileges* privileges) {
  switch (op_type) {
    case RowOperationsPB::INSERT:
      InsertIfNotPresent(privileges, WritePrivilegeType::INSERT);
      break;
    case RowOperationsPB::UPSERT:
      InsertIfNotPresent(privileges, WritePrivilegeType::INSERT);
      InsertIfNotPresent(privileges, WritePrivilegeType::UPDATE);
      break;
    case RowOperationsPB::UPDATE:
      InsertIfNotPresent(privileges, WritePrivilegeType::UPDATE);
      break;
    case RowOperationsPB::DELETE:
      InsertIfNotPresent(privileges, WritePrivilegeType::DELETE);
      break;
    default:
      LOG(DFATAL) << "Not a write operation: " << RowOperationsPB_Type_Name(op_type);
      break;
  }
}

Status WriteAuthorizationContext::CheckPrivileges() const {
  WritePrivileges required_write_privileges;
  for (const auto& op_type : requested_op_types) {
    AddWritePrivilegesForRowOperations(op_type, &required_write_privileges);
  }
  for (const auto& required_write_privilege : required_write_privileges) {
    if (!ContainsKey(write_privileges, required_write_privilege)) {
      return Status::NotAuthorized(Substitute("not authorized to $0",
          WritePrivilegeToString(required_write_privilege)));
    }
  }
  return Status::OK();
}

WriteOp::WriteOp(unique_ptr<WriteOpState> state, DriverType type)
  : Op(type, Op::WRITE_OP),
  state_(std::move(state)) {
  start_time_ = MonoTime::Now();
}

void WriteOp::NewReplicateMsg(unique_ptr<ReplicateMsg>* replicate_msg) {
  replicate_msg->reset(new ReplicateMsg);
  (*replicate_msg)->set_op_type(consensus::OperationType::WRITE_OP);
  (*replicate_msg)->mutable_write_request()->CopyFrom(*state()->request());
  if (state()->are_results_tracked()) {
    (*replicate_msg)->mutable_request_id()->CopyFrom(state()->request_id());
  }
}

Status WriteOp::Prepare() {
  TRACE_EVENT0("op", "WriteOp::Prepare");
  TRACE("PREPARE: Starting.");
  // Decode everything first so that we give up if something major is wrong.
  Schema client_schema;
  RETURN_NOT_OK_PREPEND(SchemaFromPB(state_->request()->schema(), &client_schema),
                        "Cannot decode client schema");
  if (client_schema.has_column_ids()) {
    // TODO(unknown): we have this kind of code a lot - add a new SchemaFromPB variant which
    // does this check inline.
    Status s = Status::InvalidArgument("User requests should not have Column IDs");
    state_->completion_callback()->set_error(s, TabletServerErrorPB::INVALID_SCHEMA);
    return s;
  }

  Tablet* tablet = state()->tablet_replica()->tablet();

  Status s = tablet->DecodeWriteOperations(&client_schema, state());
  if (!s.ok()) {
    // TODO(unknown): is MISMATCHED_SCHEMA always right here? probably not.
    state()->completion_callback()->set_error(s, TabletServerErrorPB::MISMATCHED_SCHEMA);
    return s;
  }

  // Authorize the request if needed.
  const auto& authz_context = state()->authz_context();
  if (authz_context) {
    Status s = authz_context->CheckPrivileges();
    if (!s.ok()) {
      state()->completion_callback()->set_error(s, TabletServerErrorPB::NOT_AUTHORIZED);
      return s;
    }
  }

  // Now acquire row locks and prepare everything for apply
  RETURN_NOT_OK(tablet->AcquireRowLocks(state()));

  TRACE("PREPARE: Finished.");
  return Status::OK();
}

void WriteOp::AbortPrepare() {
  state()->ReleaseMvccTxn(OpResult::ABORTED);
}

Status WriteOp::Start() {
  TRACE_EVENT0("op", "WriteOp::Start");
  TRACE("Start()");
  DCHECK(!state_->has_timestamp());
  DCHECK(state_->consensus_round()->replicate_msg()->has_timestamp());
  state_->set_timestamp(Timestamp(state_->consensus_round()->replicate_msg()->timestamp()));
  state_->tablet_replica()->tablet()->StartOp(state_.get());
  TRACE("Timestamp: $0", state_->tablet_replica()->clock()->Stringify(state_->timestamp()));
  return Status::OK();
}

void WriteOp::UpdatePerRowErrors() {
  // Add per-row errors to the result, update metrics.
  for (int i = 0; i < state()->row_ops().size(); ++i) {
    const RowOp* op = state()->row_ops()[i];
    if (op->result->has_failed_status()) {
      // Replicas disregard the per row errors, for now
      // TODO(unknown): check the per-row errors against the leader's, at least in debug mode
      WriteResponsePB::PerRowErrorPB* error = state()->response()->add_per_row_errors();
      error->set_row_index(i);
      error->mutable_error()->CopyFrom(op->result->failed_status());
    }

    state()->UpdateMetricsForOp(*op);
  }
}

// FIXME: Since this is called as a void in a thread-pool callback,
// it seems pointless to return a Status!
Status WriteOp::Apply(CommitMsg** commit_msg) {
  TRACE_EVENT0("op", "WriteOp::Apply");
  TRACE("APPLY: Starting.");

  if (PREDICT_FALSE(
          ANNOTATE_UNPROTECTED_READ(FLAGS_tablet_inject_latency_on_apply_write_txn_ms) > 0)) {
    TRACE("Injecting $0ms of latency due to --tablet_inject_latency_on_apply_write_txn_ms",
          FLAGS_tablet_inject_latency_on_apply_write_txn_ms);
    SleepFor(MonoDelta::FromMilliseconds(FLAGS_tablet_inject_latency_on_apply_write_txn_ms));
  }

  Tablet* tablet = state()->tablet_replica()->tablet();
  RETURN_NOT_OK(tablet->ApplyRowOperations(state()));
  TRACE("APPLY: Finished.");

  UpdatePerRowErrors();

  // Create the Commit message
  *commit_msg = google::protobuf::Arena::CreateMessage<CommitMsg>(state_->pb_arena());
  state()->ReleaseTxResultPB((*commit_msg)->mutable_result());
  (*commit_msg)->set_op_type(consensus::OperationType::WRITE_OP);

  return Status::OK();
}

void WriteOp::Finish(OpResult result) {
  TRACE_EVENT0("op", "WriteOp::Finish");

  state()->CommitOrAbort(result);

  if (PREDICT_FALSE(result == Op::ABORTED)) {
    TRACE("FINISH: Op aborted.");
    return;
  }

  DCHECK_EQ(result, Op::COMMITTED);

  TRACE("FINISH: Updating metrics.");

  TabletMetrics* metrics = state_->tablet_replica()->tablet()->metrics();
  if (metrics) {
    // TODO(unknown): should we change this so it's actually incremented by the
    // Tablet code itself instead of this wrapper code?
    metrics->rows_inserted->IncrementBy(state_->metrics().successful_inserts);
    metrics->insert_ignore_errors->IncrementBy(state_->metrics().insert_ignore_errors);
    metrics->rows_upserted->IncrementBy(state_->metrics().successful_upserts);
    metrics->rows_updated->IncrementBy(state_->metrics().successful_updates);
    metrics->rows_deleted->IncrementBy(state_->metrics().successful_deletes);

    if (type() == consensus::LEADER) {
      if (state()->external_consistency_mode() == COMMIT_WAIT) {
        metrics->commit_wait_duration->Increment(state_->metrics().commit_wait_duration_usec);
      }
      uint64_t op_duration_usec =
          (MonoTime::Now() - start_time_).ToMicroseconds();
      switch (state()->external_consistency_mode()) {
        case CLIENT_PROPAGATED:
          metrics->write_op_duration_client_propagated_consistency->Increment(op_duration_usec);
          break;
        case COMMIT_WAIT:
          metrics->write_op_duration_commit_wait_consistency->Increment(op_duration_usec);
          break;
        case UNKNOWN_EXTERNAL_CONSISTENCY_MODE:
          break;
      }
    }
  }
}

string WriteOp::ToString() const {
  MonoTime now(MonoTime::Now());
  MonoDelta d = now - start_time_;
  WallTime abs_time = WallTime_Now() - d.ToSeconds();
  string abs_time_formatted;
  StringAppendStrftime(&abs_time_formatted, "%Y-%m-%d %H:%M:%S", (time_t)abs_time, true);
  return Substitute("WriteOp [type=$0, start_time=$1, state=$2]",
                    DriverType_Name(type()), abs_time_formatted, state_->ToString());
}

WriteOpState::WriteOpState(TabletReplica* tablet_replica,
                           const tserver::WriteRequestPB *request,
                           const rpc::RequestIdPB* request_id,
                           tserver::WriteResponsePB *response,
                           boost::optional<WriteAuthorizationContext> authz_ctx)
  : OpState(tablet_replica),
    request_(DCHECK_NOTNULL(request)),
    response_(response),
    authz_context_(std::move(authz_ctx)),
    mvcc_op_(nullptr),
    schema_at_decode_time_(nullptr) {
  external_consistency_mode_ = request_->external_consistency_mode();
  if (!response_) {
    response_ = &owned_response_;
  }
  if (request_id) {
    request_id_ = *request_id;
  }
}

void WriteOpState::SetMvccTx(unique_ptr<ScopedOp> mvcc_op) {
  DCHECK(!mvcc_op_) << "Mvcc op already started/set.";
  mvcc_op_ = std::move(mvcc_op);
}

void WriteOpState::set_tablet_components(
    const scoped_refptr<const TabletComponents>& components) {
  DCHECK(!tablet_components_) << "Already set";
  DCHECK(components);
  tablet_components_ = components;
}

void WriteOpState::AcquireSchemaLock(rw_semaphore* schema_lock) {
  TRACE("Acquiring schema lock in shared mode");
  shared_lock<rw_semaphore> temp(*schema_lock);
  schema_lock_.swap(temp);
  TRACE("Acquired schema lock");
}

void WriteOpState::ReleaseSchemaLock() {
  shared_lock<rw_semaphore> temp;
  schema_lock_.swap(temp);
  TRACE("Released schema lock");
}

void WriteOpState::SetRowOps(vector<DecodedRowOperation> decoded_ops) {
  std::lock_guard<simple_spinlock> l(op_state_lock_);
  row_ops_.clear();
  row_ops_.reserve(decoded_ops.size());

  Arena* arena = this->arena();
  for (DecodedRowOperation& op : decoded_ops) {
    if (authz_context_) {
      InsertIfNotPresent(&authz_context_->requested_op_types, op.type);
    }
    row_ops_.emplace_back(arena->NewObject<RowOp>(pb_arena(), std::move(op)));
  }

  // Allocate the ProbeStats objects from the op's arena, so
  // they're all contiguous and we don't need to do any central allocation.
  stats_array_ = static_cast<ProbeStats*>(
      arena->AllocateBytesAligned(sizeof(ProbeStats) * row_ops_.size(),
                                  alignof(ProbeStats)));

  // Manually run the constructor to clear the stats to 0 before collecting them.
  for (int i = 0; i < row_ops_.size(); i++) {
    new (&stats_array_[i]) ProbeStats();
  }
}

void WriteOpState::StartApplying() {
  CHECK_NOTNULL(mvcc_op_.get())->StartApplying();
}

void WriteOpState::CommitOrAbort(Op::OpResult result) {
  ReleaseMvccTxn(result);

  TRACE("Releasing row and schema locks");
  ReleaseRowLocks();
  ReleaseSchemaLock();

  // After committing, if there is an RPC going on, the driver will respond to it.
  // That will delete the RPC request and response objects. So, NULL them here
  // so we don't read them again after they're deleted.
  ResetRpcFields();
}

void WriteOpState::ReleaseMvccTxn(Op::OpResult result) {
  if (mvcc_op_) {
    // Commit the op.
    switch (result) {
      case Op::COMMITTED:
        mvcc_op_->Commit();
        break;
      case Op::ABORTED:
        mvcc_op_->Abort();
        break;
    }
  }
  mvcc_op_.reset();
}

void WriteOpState::ReleaseTxResultPB(TxResultPB* result) const {
  result->Clear();
  result->mutable_ops()->Reserve(row_ops_.size());
  for (RowOp* op : row_ops_) {
    DCHECK_EQ(op->result->GetArena(), result->GetArena());
    result->mutable_ops()->AddAllocated(DCHECK_NOTNULL(op->result));
  }
}

void WriteOpState::UpdateMetricsForOp(const RowOp& op) {
  if (op.result->has_failed_status()) {
    return;
  }
  switch (op.decoded_op.type) {
    case RowOperationsPB::INSERT:
      op_metrics_.successful_inserts++;
      break;
    case RowOperationsPB::INSERT_IGNORE:
      if (op.error_ignored) {
        op_metrics_.insert_ignore_errors++;
      } else {
        op_metrics_.successful_inserts++;
      }
      break;
    case RowOperationsPB::UPSERT:
      op_metrics_.successful_upserts++;
      break;
    case RowOperationsPB::UPDATE:
      op_metrics_.successful_updates++;
      break;
    case RowOperationsPB::DELETE:
      op_metrics_.successful_deletes++;
      break;
    case RowOperationsPB::UNKNOWN:
    case RowOperationsPB::SPLIT_ROW:
    case RowOperationsPB::RANGE_LOWER_BOUND:
    case RowOperationsPB::RANGE_UPPER_BOUND:
    case RowOperationsPB::INCLUSIVE_RANGE_UPPER_BOUND:
    case RowOperationsPB::EXCLUSIVE_RANGE_LOWER_BOUND:
      break;
  }
}

void WriteOpState::ReleaseRowLocks() {
  // free the row locks
  for (RowOp* op : row_ops_) {
    op->row_lock.Release();
  }
}

WriteOpState::~WriteOpState() {
  Reset();
}

void WriteOpState::Reset() {
  CommitOrAbort(Op::ABORTED);
  op_metrics_.Reset();
  timestamp_ = Timestamp::kInvalidTimestamp;
  tablet_components_ = nullptr;
  schema_at_decode_time_ = nullptr;
}

void WriteOpState::ResetRpcFields() {
  std::lock_guard<simple_spinlock> l(op_state_lock_);
  request_ = nullptr;
  response_ = nullptr;
  // these are allocated from the arena, so just run the dtors.
  for (RowOp* op : row_ops_) {
    op->~RowOp();
  }
  row_ops_.clear();
}

string WriteOpState::ToString() const {
  string ts_str;
  if (has_timestamp()) {
    ts_str = timestamp().ToString();
  } else {
    ts_str = "<unassigned>";
  }

  // Stringify the actual row operations (eg INSERT/UPDATE/etc)
  string row_ops_str = "[";
  {
    std::lock_guard<simple_spinlock> l(op_state_lock_);
    const size_t kMaxToStringify = 3;
    for (int i = 0; i < std::min(row_ops_.size(), kMaxToStringify); i++) {
      if (i > 0) {
        row_ops_str.append(", ");
      }
      row_ops_str.append(row_ops_[i]->ToString(*DCHECK_NOTNULL(schema_at_decode_time_)));
    }
    if (row_ops_.size() > kMaxToStringify) {
      row_ops_str.append(", ...");
    }
    row_ops_str.append("]");
  }

  return Substitute("WriteOpState $0 [op_id=($1), ts=$2, rows=$3]",
                    this,
                    SecureShortDebugString(op_id()),
                    ts_str,
                    row_ops_str);
}

}  // namespace tablet
}  // namespace kudu
