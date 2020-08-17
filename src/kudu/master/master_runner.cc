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

#include "kudu/master/master_runner.h"

#include <cstdint>
#include <iostream>
#include <string>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/master.h"
#include "kudu/util/flags.h"
#include "kudu/util/monotime.h"
#include "kudu/util/version_info.h"

using gflags::SET_FLAGS_DEFAULT;
using std::string;
using std::to_string;

DECLARE_bool(evict_failed_followers);

DECLARE_bool(hive_metastore_sasl_enabled);
DECLARE_string(keytab_file);

DECLARE_bool(auto_rebalancing_enabled);
DECLARE_bool(raft_prepare_replacement_before_eviction);

namespace kudu {
namespace master {

// Validates that if the HMS is configured with SASL enabled, the server has a
// keytab available. This doesn't use a GROUP_FLAG_VALIDATOR because this check
// only needs to be run on a server. E.g. tools that run with the HMS don't need
// to pass in a keytab.
static Status ValidateHiveMetastoreSaslEnabled() {
    if (FLAGS_hive_metastore_sasl_enabled &&
        FLAGS_keytab_file.empty()) {
        return Status::ConfigurationError("When the Hive Metastore has SASL enabled "
                                          "(--hive_metastore_sasl_enabled), Kudu must be "
                                          "configured with a keytab (--keytab_file).");
    }
    return Status::OK();
}

void SetMasterFlagDefaults() {
  constexpr int32_t kDefaultRpcServiceQueueLength = 100;

  // Reset some default values before parsing gflags.
  CHECK_NE("", SetCommandLineOptionWithMode(
      "rpc_bind_addresses",
      strings::Substitute("0.0.0.0:$0", Master::kDefaultPort).c_str(),
      SET_FLAGS_DEFAULT));
  CHECK_NE("", SetCommandLineOptionWithMode(
      "webserver_port",
      to_string(Master::kDefaultWebPort).c_str(),
      SET_FLAGS_DEFAULT));
  // Even in a small Kudu cluster, masters might be flooded with requests coming
  // from many clients (those like GetTableSchema are rather small and can be
  // processed fast, but it might be a bunch of them coming at once).
  // In addition, TSHeartbeatRequestPB from tablet servers are put into the same
  // RPC queue (see KUDU-2955). So, it makes sense to increase the default
  // setting for the RPC service queue length.
  CHECK_NE("", SetCommandLineOptionWithMode(
      "rpc_service_queue_length",
      to_string(kDefaultRpcServiceQueueLength).c_str(),
      SET_FLAGS_DEFAULT));
  // Setting the default value of the 'force_block_cache_capacity' flag to
  // 'false' makes the corresponding group validator enforce proper settings
  // for the memory limit and the cfile cache capacity.
  CHECK_NE("", SetCommandLineOptionWithMode("force_block_cache_capacity",
                                            "false",
                                            SET_FLAGS_DEFAULT));
  // A multi-node Master leader should not evict failed Master followers
  // because there is no-one to assign replacement servers in order to maintain
  // the desired replication factor. (It's not turtles all the way down!)
  CHECK_NE("", SetCommandLineOptionWithMode("evict_failed_followers",
                                            "false",
                                            SET_FLAGS_DEFAULT));
  // SET_FLAGS_DEFAULT won't reset the flag value if it has previously been
  // set, instead it will only change the default. Because we want to ensure
  // evict_failed_followers is always false, we explicitly set the flag.
  FLAGS_evict_failed_followers = false;
}

Status RunMasterServer() {
  string nondefault_flags = GetNonDefaultFlags();
  LOG(INFO) << "Master server non-default flags:\n"
            << nondefault_flags << '\n'
            << "Master server version:\n"
            << VersionInfo::GetAllVersionInfo();

  RETURN_NOT_OK(ValidateHiveMetastoreSaslEnabled());

  Master server({});
  RETURN_NOT_OK(server.Init());
  RETURN_NOT_OK(server.Start());

  while (true) {
    SleepFor(MonoDelta::FromSeconds(60));
  }
}

} // namespace master
} // namespace kudu
