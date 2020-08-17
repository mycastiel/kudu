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
#pragma once

#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include <boost/optional/optional.hpp>

#include "kudu/gutil/ref_counted.h"
#include "kudu/transactions/txn_status_entry.h"
#include "kudu/transactions/txn_status_tablet.h"
#include "kudu/util/locks.h"
#include "kudu/util/status.h"

namespace kudu {

namespace tablet {
class TabletReplica;
} // namespace tablet

namespace transactions {

class TxnStatusEntryPB;

// Maps the transaction ID to the corresponding TransactionEntry.
typedef std::unordered_map<int64_t, scoped_refptr<TransactionEntry>> TransactionsMap;

// Maps the transaction ID to the transaction's participants' tablet IDs. This
// is convenient to use in testing, given its relative ease of construction.
typedef std::map<int64_t, std::vector<std::string>> ParticipantIdsByTxnId;

// Visitor used to iterate over and load into memory the existing state from a
// status tablet.
class TxnStatusManagerBuildingVisitor : public TransactionsVisitor {
 public:
  // Builds a TransactionEntry for the given metadata and keeps track of it in
  // txns_by_id_. This is not thread-safe -- callers should ensure only a
  // single thread calls it at once.
  void VisitTransactionEntries(int64_t txn_id, TxnStatusEntryPB status_entry_pb,
                               std::vector<ParticipantIdAndPB> participants) override;

  // Releases the transactions map to the caller. Should only be called once
  // per call to VisitTransactionEntries().
  void Release(int64_t* highest_txn_id, TransactionsMap* txns_by_id);
 private:
  int64_t highest_txn_id_ = -1;
  TransactionsMap txns_by_id_;
};

// Manages ongoing transactions and participants therein, backed by an
// underlying tablet.
class TxnStatusManager {
 public:
  explicit TxnStatusManager(tablet::TabletReplica* tablet_replica)
      : highest_txn_id_(-1),
        status_tablet_(tablet_replica) {}
  // Loads the contents of the status tablet into memory.
  Status LoadFromTablet();

  // Writes an entry to the status tablet and creates a transaction in memory.
  // Returns an error if a higher transaction ID has already been attempted
  // (even if that attempt failed), which helps ensure that at most one call to
  // this method will succeed for a given transaction ID.
  //
  // TODO(awong): consider computing the next available transaction ID in this
  // partition and using it in case this transaction is already used, or having
  // callers forward a request for the next-highest transaction ID.
  Status BeginTransaction(int64_t txn_id, const std::string& user);

  // Begins committing the given transaction, returning an error if the
  // transaction doesn't exist, isn't open, or isn't owned by the given user.
  Status BeginCommitTransaction(int64_t txn_id, const std::string& user);

  // Finalizes the commit of the transaction, returning an error if the
  // transaction isn't in an appropraite state.
  //
  // Unlike the other transaction life-cycle calls, this isn't user-initiated,
  // so it doesn't take a user.
  //
  // TODO(awong): add a commit timestamp.
  Status FinalizeCommitTransaction(int64_t txn_id);

  // Aborts the given transaction, returning an error if the transaction
  // doesn't exist, is committed or not yet opened, or isn't owned by the given
  // user.
  Status AbortTransaction(int64_t txn_id, const std::string& user);

  // Creates an in-memory participant, writes an entry to the status table, and
  // attaches the in-memory participant to the transaction.
  //
  // If the transaction is open, it is ensured to be active for the duration of
  // this call. Returns an error if the given transaction isn't open.
  Status RegisterParticipant(int64_t txn_id, const std::string& tablet_id,
                             const std::string& user);

  // Populates a map from transaction ID to the sorted list of participants
  // associated with that transaction ID.
  ParticipantIdsByTxnId GetParticipantsByTxnIdForTests() const;

  int64_t highest_txn_id() const {
    std::lock_guard<simple_spinlock> l(lock_);
    return highest_txn_id_;
  }

 private:
  // Returns the transaction entry, returning an error if the transaction ID
  // doesn't exist or if 'user' is specified but isn't the owner of the
  // transaction.
  Status GetTransaction(int64_t txn_id, const boost::optional<std::string>& user,
                        scoped_refptr<TransactionEntry>* txn) const;

  // Protects 'highest_txn_id_' and 'txns_by_id_'.
  mutable simple_spinlock lock_;

  // The highest transaction ID seen by this status manager so far. Requests to
  // create a new transaction must provide an ID higher than this ID.
  int64_t highest_txn_id_;

  // Tracks the currently on-going transactions.
  TransactionsMap txns_by_id_;

  // The access to underlying storage.
  TxnStatusTablet status_tablet_;
};

} // namespace transactions
} // namespace kudu
