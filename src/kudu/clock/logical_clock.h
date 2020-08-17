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
#include <string>

#include "kudu/clock/clock.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/timestamp.h"
#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/metrics.h"
#include "kudu/util/status.h"

namespace kudu {

class MonoTime;

namespace clock {

// An implementation of Clock that behaves as a plain Lamport Clock.
// In a single node, single tablet, setting this generates exactly the
// same Timestamp sequence as the original MvccManager did, but it can be
// updated to make sure replicas generate new timestamps on becoming leader.
// This can be used as a deterministic timestamp generator that has the same
// consistency properties as a HybridTime clock.
//
// The Wait* methods are unavailable in this implementation and will
// return Status::ServiceUnavailable().
//
// NOTE: this class is thread safe.
class LogicalClock : public Clock {
 public:
  // Create logical clock starting with the given timestamp.
  explicit LogicalClock(const Timestamp& timestamp,
                        const scoped_refptr<MetricEntity>& metric_entity = {});

  Status Init() override { return Status::OK(); }

  Timestamp Now() override;

  // In the logical clock this call is equivalent to Now();
  Timestamp NowLatest() override;

  Status Update(const Timestamp& to_update) override;

  // The Wait*() functions are not available for this clock.
  Status WaitUntilAfter(const Timestamp& then,
                        const MonoTime& deadline) override;
  Status WaitUntilAfterLocally(const Timestamp& then,
                               const MonoTime& deadline) override;

  bool IsAfter(Timestamp t) override;

  std::string Stringify(Timestamp timestamp) override;

  // Used to get the timestamp without incrementing the logical component.
  // Mostly used for tests/metrics.
  uint64_t GetCurrentTime();

  // Logical clock doesn't support COMMIT_WAIT.
  bool SupportsExternalConsistencyMode(ExternalConsistencyMode mode) const override {
    return mode != COMMIT_WAIT;
  }

 private:
  base::subtle::Atomic64 now_;

  FunctionGaugeDetacher metric_detacher_;
};

}  // namespace clock
}  // namespace kudu
