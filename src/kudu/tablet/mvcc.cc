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

#include "kudu/tablet/mvcc.h"

#include <algorithm>
#include <mutex>
#include <ostream>
#include <utility>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/strcat.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/fault_injection.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/monotime.h"

DEFINE_int32(inject_latency_ms_before_starting_txn, 0,
             "Amount of latency in ms to inject before registering "
             "an op with MVCC.");
TAG_FLAG(inject_latency_ms_before_starting_txn, advanced);
TAG_FLAG(inject_latency_ms_before_starting_txn, hidden);

namespace kudu {
namespace tablet {

using strings::Substitute;

MvccManager::MvccManager()
  : new_op_timestamp_exc_lower_bound_(Timestamp::kMin),
    earliest_in_flight_(Timestamp::kMax),
    open_(true) {
  cur_snap_.all_committed_before_ = Timestamp::kInitialTimestamp;
  cur_snap_.none_committed_at_or_after_ = Timestamp::kInitialTimestamp;
}

Status MvccManager::CheckIsCleanTimeInitialized() const {
  if (GetCleanTimestamp() == Timestamp::kInitialTimestamp) {
    return Status::Uninitialized("clean time has not yet been initialized");
  }
  return Status::OK();
}

void MvccManager::StartOp(Timestamp timestamp) {
  MAYBE_INJECT_RANDOM_LATENCY(FLAGS_inject_latency_ms_before_starting_txn);
  std::lock_guard<LockType> l(lock_);
  CHECK(!cur_snap_.IsCommitted(timestamp)) <<
      Substitute("Trying to start a new txn at an already committed "
                 "timestamp: $0, current MVCC snapshot: $1",
                 timestamp.ToString(), cur_snap_.ToString());
  CHECK(InitOpUnlocked(timestamp)) <<
      Substitute("There is already a txn with timestamp: $0 in flight, or "
                 "this timestamp is below or equal to the exclusive lower "
                 "bound for new op timestamps. Current lower bound: "
                 "$1, current MVCC snapshot: $2", timestamp.ToString(),
                 new_op_timestamp_exc_lower_bound_.ToString(),
                 cur_snap_.ToString());
}

void MvccManager::StartApplyingOp(Timestamp timestamp) {
  std::lock_guard<LockType> l(lock_);
  auto it = timestamps_in_flight_.find(timestamp.value());
  if (PREDICT_FALSE(it == timestamps_in_flight_.end())) {
    LOG(FATAL) << "Cannot mark timestamp " << timestamp.ToString() << " as APPLYING: "
               << "not in the in-flight map.";
  }

  TxnState cur_state = it->second;
  if (PREDICT_FALSE(cur_state != RESERVED)) {
    LOG(FATAL) << "Cannot mark timestamp " << timestamp.ToString() << " as APPLYING: "
               << "wrong state: " << cur_state;
  }

  it->second = APPLYING;
}

bool MvccManager::InitOpUnlocked(const Timestamp& timestamp) {
  // Ensure we're not trying to start an op that falls before our lower
  // bound.
  if (PREDICT_FALSE(timestamp <= new_op_timestamp_exc_lower_bound_)) {
    return false;
  }

  if (timestamp < earliest_in_flight_) {
    earliest_in_flight_ = timestamp;
  }

  return InsertIfNotPresent(&timestamps_in_flight_, timestamp.value(), RESERVED);
}

void MvccManager::AbortOp(Timestamp timestamp) {
  std::lock_guard<LockType> l(lock_);

  // Remove from our in-flight list.
  TxnState old_state = RemoveInFlightAndGetStateUnlocked(timestamp);

  // If the tablet is shutting down, we can ignore the state of the
  // ops.
  if (PREDICT_FALSE(!open_.load())) {
    LOG(WARNING) << "aborting op with timestamp " << timestamp.ToString()
        << " in state " << old_state << "; MVCC is closed";
    return;
  }

  CHECK_EQ(old_state, RESERVED) << "op with timestamp " << timestamp.ToString()
                                << " cannot be aborted in state " << old_state;

  // If we're aborting the earliest op that was in flight,
  // update our cached value.
  if (earliest_in_flight_ == timestamp) {
    AdvanceEarliestInFlightTimestamp();
  }
}

void MvccManager::CommitOp(Timestamp timestamp) {
  std::lock_guard<LockType> l(lock_);

  // Commit the op, but do not adjust 'all_committed_before_', that will
  // be done with a separate OfflineAdjustCurSnap() call.
  bool was_earliest = false;
  CommitOpUnlocked(timestamp, &was_earliest);

  // NOTE: we should have pushed the lower bound forward before committing, but
  // we may not have in tests.
  if (was_earliest && new_op_timestamp_exc_lower_bound_ >= timestamp) {

    // If this op was the earliest in-flight, we might have to adjust
    // the "clean" timestamp.
    AdjustCleanTimeUnlocked();
  }
}

MvccManager::TxnState MvccManager::RemoveInFlightAndGetStateUnlocked(Timestamp ts) {
  DCHECK(lock_.is_locked());

  auto it = timestamps_in_flight_.find(ts.value());
  if (it == timestamps_in_flight_.end()) {
    LOG(FATAL) << "Trying to remove timestamp which isn't in the in-flight set: "
               << ts.ToString();
  }
  TxnState state = it->second;
  timestamps_in_flight_.erase(it);
  return state;
}

void MvccManager::CommitOpUnlocked(Timestamp timestamp,
                                   bool* was_earliest_in_flight) {
  *was_earliest_in_flight = earliest_in_flight_ == timestamp;

  // Remove from our in-flight list.
  TxnState old_state = RemoveInFlightAndGetStateUnlocked(timestamp);
  CHECK_EQ(old_state, APPLYING)
    << "Trying to commit an op which never entered APPLYING state: "
    << timestamp.ToString() << " state=" << old_state;

  // Add to snapshot's committed list
  cur_snap_.AddCommittedTimestamp(timestamp);

  // If we're committing the earliest op that was in flight,
  // update our cached value.
  if (*was_earliest_in_flight) {
    AdvanceEarliestInFlightTimestamp();
  }
}

void MvccManager::AdvanceEarliestInFlightTimestamp() {
  if (timestamps_in_flight_.empty()) {
    earliest_in_flight_ = Timestamp::kMax;
  } else {
    earliest_in_flight_ = Timestamp(std::min_element(timestamps_in_flight_.begin(),
                                                     timestamps_in_flight_.end())->first);
  }
}

void MvccManager::AdjustNewOpLowerBound(Timestamp timestamp) {
  std::lock_guard<LockType> l(lock_);
  // No more ops will start with a timestamp that is lower than or
  // equal to 'timestamp', so we adjust the snapshot accordingly.
  if (PREDICT_TRUE(new_op_timestamp_exc_lower_bound_ <= timestamp)) {
    DVLOG(4) << "Adjusting new op lower bound to: " << timestamp;
    new_op_timestamp_exc_lower_bound_ = timestamp;
  } else {
    // Note: Getting here means that we are about to apply an op out of
    // order. This out-of-order applying is only safe because concurrrent
    // ops are guaranteed to not affect the same state based on locks
    // taken before starting the op (e.g. row locks, schema locks).
    KLOG_EVERY_N(INFO, 10) <<
        Substitute("Tried to move back new op lower bound from $0 to $1. "
                   "Current Snapshot: $2", new_op_timestamp_exc_lower_bound_.ToString(),
                   timestamp.ToString(), cur_snap_.ToString());
    return;
  }

  AdjustCleanTimeUnlocked();
}

// Remove any elements from 'v' which are < the given watermark.
static void FilterTimestamps(std::vector<Timestamp::val_type>* v,
                             Timestamp::val_type watermark) {
  int j = 0;
  for (const auto& ts : *v) {
    if (ts >= watermark) {
      (*v)[j++] = ts;
    }
  }
  v->resize(j);
}

void MvccManager::Close() {
  open_.store(false);
  std::lock_guard<LockType> l(lock_);
  auto iter = waiters_.begin();
  while (iter != waiters_.end()) {
    auto* waiter = *iter;
    iter = waiters_.erase(iter);
    waiter->latch->CountDown();
  }
}

void MvccManager::AdjustCleanTimeUnlocked() {
  DCHECK(lock_.is_locked());

  // There are two possibilities:
  //
  // 1) We still have an in-flight op earlier than
  //    'new_op_timestamp_exc_lower_bound_'. In this case, we update the
  //    watermark to that op's timestamp.
  //
  // 2) There are no in-flight ops earlier than
  //    'new_op_timestamp_exc_lower_bound_'. In this case, we update the
  //    watermark to that lower bound, since we know that no new ops
  //    can start with an earlier timestamp.
  //    NOTE: there may still be in-flight ops with future timestamps
  //    due to commit-wait ops which start in the future.
  //
  // In either case, we have to add the newly committed ts only if it remains higher
  // than the new watermark.

  if (earliest_in_flight_ < new_op_timestamp_exc_lower_bound_) {
    cur_snap_.all_committed_before_ = earliest_in_flight_;
  } else {
    cur_snap_.all_committed_before_ = new_op_timestamp_exc_lower_bound_;
  }

  DVLOG(4) << "Adjusted clean time to: " << cur_snap_.all_committed_before_;

  // Filter out any committed timestamps that now fall below the watermark
  FilterTimestamps(&cur_snap_.committed_timestamps_, cur_snap_.all_committed_before_.value());

  // If the current snapshot doesn't have any committed timestamps, then make sure we still
  // advance the 'none_committed_at_or_after_' watermark so that it never falls below
  // 'all_committed_before_'.
  if (cur_snap_.committed_timestamps_.empty()) {
    cur_snap_.none_committed_at_or_after_ = cur_snap_.all_committed_before_;
  }

  // it may also have unblocked some waiters.
  // Check if someone is waiting for ops to be committed.
  if (PREDICT_FALSE(!waiters_.empty())) {
    auto iter = waiters_.begin();
    while (iter != waiters_.end()) {
      auto* waiter = *iter;
      if (IsDoneWaitingUnlocked(*waiter)) {
        iter = waiters_.erase(iter);
        waiter->latch->CountDown();
        continue;
      }
      iter++;
    }
  }
}

Status MvccManager::WaitUntil(WaitFor wait_for, Timestamp ts, const MonoTime& deadline) const {
  TRACE_EVENT2("tablet", "MvccManager::WaitUntil",
               "wait_for", wait_for == ALL_COMMITTED ? "all_committed" : "none_applying",
               "ts", ts.ToUint64());

  // If MVCC is closed, there's no point in waiting.
  RETURN_NOT_OK(CheckOpen());
  CountDownLatch latch(1);
  WaitingState waiting_state;
  {
    waiting_state.timestamp = ts;
    waiting_state.latch = &latch;
    waiting_state.wait_for = wait_for;

    std::lock_guard<LockType> l(lock_);
    if (IsDoneWaitingUnlocked(waiting_state)) return Status::OK();
    waiters_.push_back(&waiting_state);
  }
  if (waiting_state.latch->WaitUntil(deadline)) {
    // If the wait ended because MVCC is shutting down, return an error.
    return CheckOpen();
  }
  // We timed out. We need to clean up our entry in the waiters_ array.

  std::lock_guard<LockType> l(lock_);
  // It's possible that we got notified while we were re-acquiring the lock. In
  // that case, we have no cleanup to do.
  if (waiting_state.latch->count() == 0) {
    return CheckOpen();
  }

  waiters_.erase(std::find(waiters_.begin(), waiters_.end(), &waiting_state));
  return Status::TimedOut(Substitute("Timed out waiting for all ops with ts < $0 to $1",
                                     ts.ToString(),
                                     wait_for == ALL_COMMITTED ? "commit" : "finish applying"));
}

bool MvccManager::IsDoneWaitingUnlocked(const WaitingState& waiter) const {
  switch (waiter.wait_for) {
    case ALL_COMMITTED:
      return AreAllOpsCommittedUnlocked(waiter.timestamp);
    case NONE_APPLYING:
      return !AnyApplyingAtOrBeforeUnlocked(waiter.timestamp);
  }
  LOG(FATAL); // unreachable
}

Status MvccManager::CheckOpen() const {
  if (PREDICT_TRUE(open_.load())) {
    return Status::OK();
  }
  return Status::Aborted("MVCC is closed");
}

bool MvccManager::AreAllOpsCommittedUnlocked(Timestamp ts) const {
  // If ts is before the 'all_committed_before_' watermark on the current snapshot then
  // all ops before it are committed.
  if (ts < cur_snap_.all_committed_before_) return true;

  // We might not have moved 'cur_snap_.all_committed_before_' (the clean time) but 'ts'
  // might still come before any possible in-flights.
  return ts < earliest_in_flight_;
}

bool MvccManager::AnyApplyingAtOrBeforeUnlocked(Timestamp ts) const {
  // TODO(todd) this is not actually checking on the applying ops, it's checking on
  // _all in-flight_. Is this a bug?
  for (const InFlightMap::value_type entry : timestamps_in_flight_) {
    if (entry.first <= ts.value()) {
      return true;
    }
  }
  return false;
}

void MvccManager::TakeSnapshot(MvccSnapshot *snap) const {
  std::lock_guard<LockType> l(lock_);
  *snap = cur_snap_;
}

Status MvccManager::WaitForSnapshotWithAllCommitted(Timestamp timestamp,
                                                    MvccSnapshot* snapshot,
                                                    const MonoTime& deadline) const {
  TRACE_EVENT0("tablet", "MvccManager::WaitForSnapshotWithAllCommitted");

  RETURN_NOT_OK(WaitUntil(ALL_COMMITTED, timestamp, deadline));
  *snapshot = MvccSnapshot(timestamp);
  return Status::OK();
}

Status MvccManager::WaitForApplyingOpsToCommit() const {
  TRACE_EVENT0("tablet", "MvccManager::WaitForApplyingOpsToCommit");
  RETURN_NOT_OK(CheckOpen());

  // Find the highest timestamp of an APPLYING op.
  Timestamp wait_for = Timestamp::kMin;
  {
    std::lock_guard<LockType> l(lock_);
    for (const InFlightMap::value_type entry : timestamps_in_flight_) {
      if (entry.second == APPLYING) {
        wait_for = Timestamp(std::max(entry.first, wait_for.value()));
      }
    }
  }

  // Wait until there are no ops applying with that timestamp or below. It's
  // possible that we're a bit conservative here - more ops may enter the
  // APPLYING set while we're waiting, but we will eventually succeed.
  if (wait_for == Timestamp::kMin) {
    // None were APPLYING: we can just return.
    return Status::OK();
  }
  return WaitUntil(NONE_APPLYING, wait_for, MonoTime::Max());
}

Timestamp MvccManager::GetCleanTimestamp() const {
  std::lock_guard<LockType> l(lock_);
  return cur_snap_.all_committed_before_;
}

void MvccManager::GetApplyingOpsTimestamps(std::vector<Timestamp>* timestamps) const {
  std::lock_guard<LockType> l(lock_);
  timestamps->reserve(timestamps_in_flight_.size());
  for (const InFlightMap::value_type entry : timestamps_in_flight_) {
    if (entry.second == APPLYING) {
      timestamps->push_back(Timestamp(entry.first));
    }
  }
}

MvccManager::~MvccManager() {
  CHECK(waiters_.empty());
}

////////////////////////////////////////////////////////////
// MvccSnapshot
////////////////////////////////////////////////////////////

MvccSnapshot::MvccSnapshot()
  : all_committed_before_(Timestamp::kInitialTimestamp),
    none_committed_at_or_after_(Timestamp::kInitialTimestamp) {
}

MvccSnapshot::MvccSnapshot(const MvccManager &manager) {
  manager.TakeSnapshot(this);
}

MvccSnapshot::MvccSnapshot(const Timestamp& timestamp)
  : all_committed_before_(timestamp),
    none_committed_at_or_after_(timestamp) {
 }

MvccSnapshot MvccSnapshot::CreateSnapshotIncludingAllOps() {
  return MvccSnapshot(Timestamp::kMax);
}

MvccSnapshot MvccSnapshot::CreateSnapshotIncludingNoOps() {
  return MvccSnapshot(Timestamp::kMin);
}

bool MvccSnapshot::IsCommittedFallback(const Timestamp& timestamp) const {
  for (const Timestamp::val_type& v : committed_timestamps_) {
    if (v == timestamp.value()) return true;
  }

  return false;
}

bool MvccSnapshot::MayHaveCommittedOpsAtOrAfter(const Timestamp& timestamp) const {
  return timestamp < none_committed_at_or_after_;
}

bool MvccSnapshot::MayHaveUncommittedOpsAtOrBefore(const Timestamp& timestamp) const {
  // The snapshot may have uncommitted ops before 'timestamp' if:
  // - 'all_committed_before_' comes before 'timestamp'
  // - 'all_committed_before_' is precisely 'timestamp' but 'timestamp' isn't in the
  //   committed set.
  return timestamp > all_committed_before_ ||
      (timestamp == all_committed_before_ && !IsCommittedFallback(timestamp));
}

std::string MvccSnapshot::ToString() const {
  std::string ret("MvccSnapshot[committed={T|");

  if (committed_timestamps_.size() == 0) {
    StrAppend(&ret, "T < ", all_committed_before_.ToString(),"}]");
    return ret;
  }
  StrAppend(&ret, "T < ", all_committed_before_.ToString(),
            " or (T in {");

  bool first = true;
  for (Timestamp::val_type t : committed_timestamps_) {
    if (!first) {
      ret.push_back(',');
    }
    first = false;
    StrAppend(&ret, t);
  }
  ret.append("})}]");
  return ret;
}

void MvccSnapshot::AddCommittedTimestamps(const std::vector<Timestamp>& timestamps) {
  for (const Timestamp& ts : timestamps) {
    AddCommittedTimestamp(ts);
  }
}

void MvccSnapshot::AddCommittedTimestamp(Timestamp timestamp) {
  if (IsCommitted(timestamp)) return;

  committed_timestamps_.push_back(timestamp.value());

  // If this is a new upper bound commit mark, update it.
  if (none_committed_at_or_after_ <= timestamp) {
    none_committed_at_or_after_ = Timestamp(timestamp.value() + 1);
  }
}

bool MvccSnapshot::Equals(const MvccSnapshot& other) const {
  if (all_committed_before_ != other.all_committed_before_) {
    return false;
  }
  if (none_committed_at_or_after_ != other.none_committed_at_or_after_) {
    return false;
  }
  return committed_timestamps_ == other.committed_timestamps_;
}

////////////////////////////////////////////////////////////
// ScopedOp
////////////////////////////////////////////////////////////
ScopedOp::ScopedOp(MvccManager* manager, Timestamp timestamp)
  : done_(false),
    manager_(DCHECK_NOTNULL(manager)),
    timestamp_(timestamp) {
  manager_->StartOp(timestamp);
}

ScopedOp::~ScopedOp() {
  if (!done_) {
    Abort();
  }
}

void ScopedOp::StartApplying() {
  manager_->StartApplyingOp(timestamp_);
}

void ScopedOp::Commit() {
  manager_->CommitOp(timestamp_);
  done_ = true;
}

void ScopedOp::Abort() {
  manager_->AbortOp(timestamp_);
  done_ = true;
}

} // namespace tablet
} // namespace kudu
