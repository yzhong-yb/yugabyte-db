// Copyright (c) YugaByte, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied.  See the License for the specific language governing permissions and limitations
// under the License.

#include "yb/yql/pgwrapper/pg_wrapper.h"

#include <signal.h>

#include <fstream>
#include <random>
#include <regex>
#include <string>
#include <thread>
#include <vector>

#include <boost/algorithm/string.hpp>

#include "yb/tserver/tablet_server_interface.h"
#include "yb/util/env_util.h"
#include "yb/util/errno.h"
#include "yb/util/flags.h"
#include "yb/util/logging.h"
#include "yb/util/net/net_util.h"
#include "yb/util/path_util.h"
#include "yb/util/pg_util.h"
#include "yb/util/result.h"
#include "yb/util/scope_exit.h"
#include "yb/util/status_format.h"
#include "yb/util/status_log.h"
#include "yb/util/string_util.h"
#include "yb/util/subprocess.h"
#include "yb/util/thread.h"

DEFINE_UNKNOWN_string(pg_proxy_bind_address, "", "Address for the PostgreSQL proxy to bind to");
DEFINE_UNKNOWN_string(postmaster_cgroup, "", "cgroup to add postmaster process to");
DEFINE_UNKNOWN_bool(pg_transactions_enabled, true,
            "True to enable transactions in YugaByte PostgreSQL API.");
DEFINE_UNKNOWN_string(yb_backend_oom_score_adj, "900",
              "oom_score_adj of postgres backends in linux environments");
DEFINE_UNKNOWN_bool(yb_pg_terminate_child_backend, false,
            "Terminate other active server processes when a backend is killed");
DEFINE_UNKNOWN_bool(pg_verbose_error_log, false,
            "True to enable verbose logging of errors in PostgreSQL server");
DEFINE_UNKNOWN_int32(pgsql_proxy_webserver_port, 13000, "Webserver port for PGSQL");

DEFINE_test_flag(bool, pg_collation_enabled, true,
                 "True to enable collation support in YugaByte PostgreSQL.");
// Default to 5MB
DEFINE_UNKNOWN_string(
    pg_mem_tracker_tcmalloc_gc_release_bytes, std::to_string(5 * 1024 * 1024),
    "Overriding the gflag mem_tracker_tcmalloc_gc_release_bytes "
    "defined in mem_tracker.cc. The overriding value is specifically "
    "set for Postgres backends");

DEFINE_RUNTIME_string(pg_mem_tracker_update_consumption_interval_us, std::to_string(50 * 1000),
    "Interval that is used to update memory consumption from external source. "
    "For instance from tcmalloc statistics. This interval is for Postgres backends only");

DECLARE_string(metric_node_name);
TAG_FLAG(pg_transactions_enabled, advanced);
TAG_FLAG(pg_transactions_enabled, hidden);

DEFINE_UNKNOWN_bool(pg_stat_statements_enabled, true,
            "True to enable statement stats in PostgreSQL server");
TAG_FLAG(pg_stat_statements_enabled, advanced);
TAG_FLAG(pg_stat_statements_enabled, hidden);

// Top-level postgres configuration flags.
DEFINE_UNKNOWN_bool(ysql_enable_auth, false,
              "True to enforce password authentication for all connections");

// Catch-all postgres configuration flags.
DEFINE_UNKNOWN_string(ysql_pg_conf_csv, "",
              "CSV formatted line represented list of postgres setting assignments");
DEFINE_UNKNOWN_string(ysql_hba_conf_csv, "",
              "CSV formatted line represented list of postgres hba rules (in order)");
TAG_FLAG(ysql_hba_conf_csv, sensitive_info);

DEFINE_UNKNOWN_string(ysql_pg_conf, "",
              "Deprecated, use the `ysql_pg_conf_csv` flag instead. " \
              "Comma separated list of postgres setting assignments");
DEFINE_UNKNOWN_string(ysql_hba_conf, "",
              "Deprecated, use `ysql_hba_conf_csv` flag instead. " \
              "Comma separated list of postgres hba rules (in order)");
TAG_FLAG(ysql_hba_conf, sensitive_info);

// gFlag wrappers over Postgres GUC parameter.
// The value type should match the GUC parameter, or it should be a string, in which case Postgres
// will convert it to the correct type.
// The default values of gFlag are visible to customers via flags metadata xml, documentation, and
// platform UI. So, it's important to keep these values as accurate as possible. The default_value
// or target_value (for AutoFlags) should match the default specified in guc.c.
// Use an empty string or 0 for parameters like timezone and max_connections whose default is
// computed at runtime so that they show up as an undefined value instead of an incorrect value. If
// 0 is a valid value for the parameter, then use an empty string. These are enforced by the
// PgWrapperFlagsTest.VerifyGFlagDefaults test.
#define DEFINE_NON_RUNTIME_PG_FLAG(type, name, default_value, description) \
  BOOST_PP_CAT(DEFINE_NON_RUNTIME_, type)(BOOST_PP_CAT(ysql_, name), default_value, description); \
  _TAG_FLAG(BOOST_PP_CAT(ysql_, name), ::yb::FlagTag::kPg, pg)

#define DEFINE_RUNTIME_PG_FLAG(type, name, default_value, description) \
  BOOST_PP_CAT(DEFINE_RUNTIME_, type)(BOOST_PP_CAT(ysql_, name), default_value, description); \
  _TAG_FLAG(BOOST_PP_CAT(ysql_, name), ::yb::FlagTag::kPg, pg)

#define DEFINE_RUNTIME_AUTO_PG_FLAG(type, name, flag_class, initial_val, target_val, description) \
  BOOST_PP_CAT(DEFINE_RUNTIME_AUTO_, type)(ysql_##name, flag_class, initial_val, target_val, \
                                           description); \
  _TAG_FLAG(BOOST_PP_CAT(ysql_, name), ::yb::FlagTag::kPg, pg)

DEFINE_RUNTIME_PG_FLAG(string, timezone, "",
    "Overrides the default ysql timezone for displaying and interpreting timestamps. If no value "
    "is provided, Postgres will determine one based on the environment");

DEFINE_RUNTIME_PG_FLAG(string, datestyle,
    "ISO, MDY", "The ysql display format for date and time values");

DEFINE_NON_RUNTIME_PG_FLAG(int32, max_connections, 0,
    "Overrides the maximum number of concurrent ysql connections. If set to 0, Postgres will "
    "dynamically determine a platform-specific value");

DEFINE_RUNTIME_PG_FLAG(string, default_transaction_isolation,
    "read committed", "The ysql transaction isolation level");

DEFINE_RUNTIME_PG_FLAG(string, log_statement,
    "none", "Sets which types of ysql statements should be logged");

DEFINE_RUNTIME_PG_FLAG(string, log_min_messages,
    "warning", "Sets the lowest ysql message level to log");

DEFINE_RUNTIME_PG_FLAG(int32, log_min_duration_statement, -1,
    "Sets the duration of each completed ysql statement to be logged if the statement ran for at "
    "least the specified number of milliseconds. Zero prints all queries. -1 turns this feature "
    "off.");

DEFINE_RUNTIME_PG_FLAG(bool, yb_enable_memory_tracking, true,
    "Enables tracking of memory consumption of the PostgreSQL process. This enhances garbage "
    "collection behaviour and memory usage observability.");

DEFINE_RUNTIME_AUTO_PG_FLAG(bool, yb_enable_expression_pushdown, kLocalVolatile, false, true,
    "Push supported expressions from ysql down to DocDB for evaluation.");

DEFINE_RUNTIME_PG_FLAG(bool, yb_enable_aggregate_pushdown, true,
    "Push supported aggregate from ysql down to DocDB for partial evaluation.");

DEFINE_RUNTIME_AUTO_PG_FLAG(bool, yb_pushdown_strict_inequality, kLocalVolatile, false, true,
    "Push down strict inequality filters");

DEFINE_RUNTIME_AUTO_PG_FLAG(bool, yb_enable_hash_batch_in, kLocalVolatile, false, true,
    "Enable batching of hash in queries.");

DEFINE_RUNTIME_AUTO_PG_FLAG(bool, yb_bypass_cond_recheck, kLocalVolatile, false, true,
    "Bypass index condition recheck at the YSQL layer if the condition was pushed down.");

DEFINE_RUNTIME_PG_FLAG(int32, yb_index_state_flags_update_delay, 0,
    "Delay in milliseconds between stages of online index build. For testing purposes.");

DEFINE_RUNTIME_PG_FLAG(int32, yb_bnl_batch_size, 1,
    "Batch size of nested loop joins.");

DEFINE_RUNTIME_PG_FLAG(string, yb_xcluster_consistency_level, "database",
    "Controls the consistency level of xCluster replicated databases. Valid values are "
    "\"database\" and \"tablet\".");

DEFINE_RUNTIME_PG_FLAG(string, yb_test_block_index_phase, "",
    "Block the given index phase from proceeding. Valid names are indisready, build,"
    " indisvalid and finish. For testing purposes.");

DEFINE_RUNTIME_AUTO_PG_FLAG(bool, yb_enable_sequence_pushdown, kLocalVolatile, false, true,
    "Allow nextval() to fetch the value range and advance the sequence value "
    "in a single operation");

DEFINE_RUNTIME_PG_FLAG(bool, yb_disable_wait_for_backends_catalog_version, false,
    "Disable waiting for backends to have up-to-date pg_catalog. This could cause correctness"
    " issues, which could be mitigated by setting high ysql_yb_index_state_flags_update_delay."
    " Although it is runtime-settable, the effects won't take place for any in-progress"
    " queries.");

static bool ValidateXclusterConsistencyLevel(const char* flagname, const std::string& value) {
  if (value != "database" && value != "tablet") {
    fprintf(
        stderr, "Invalid value for --%s: %s, must be 'database' or 'tablet'\n", flagname,
        value.c_str());
    return false;
  }
  return true;
}

DEFINE_validator(ysql_yb_xcluster_consistency_level, &ValidateXclusterConsistencyLevel);

using gflags::CommandLineFlagInfo;
using std::string;
using std::vector;

using namespace std::literals;  // NOLINT

namespace yb {
namespace pgwrapper {

namespace {

Status WriteConfigFile(const string& path, const vector<string>& lines) {
  std::ofstream conf_file;
  conf_file.open(path, std::ios_base::out | std::ios_base::trunc);
  if (!conf_file) {
    return STATUS_FORMAT(
        IOError,
        "Failed to write ysql config file '%s': errno=$0: $1",
        path,
        errno,
        ErrnoToString(errno));
  }

  conf_file << "# This is an autogenerated file, do not edit manually!" << std::endl;
  for (const auto& line : lines) {
    conf_file << line << std::endl;
  }

  conf_file.close();

  return Status::OK();
}

void ReadCommaSeparatedValues(const string& src, vector<string>* lines) {
  vector<string> new_lines;
  boost::split(new_lines, src, boost::is_any_of(","));
  lines->insert(lines->end(), new_lines.begin(), new_lines.end());
}

void MergeSharedPreloadLibraries(const string& src, vector<string>* defaults) {
  string copy = boost::replace_all_copy(src, " ", "");
  copy = boost::erase_first_copy(copy, "shared_preload_libraries");
  // According to the documentation in postgresql.conf file,
  // the '=' is optional hence it needs to be handled separately.
  copy = boost::erase_first_copy(copy, "=");
  copy = boost::trim_copy_if(copy, boost::is_any_of("'\""));
  vector<string> new_items;
  boost::split(new_items, copy, boost::is_any_of(","));
  // Remove empty elements, makes it safe to use with empty user
  // provided shared_preload_libraries, for example,
  // if the value was provided via environment variable, example:
  //
  //   --ysql_pg_conf="shared_preload_libraries='$UNSET_VALUE'"
  //
  // Alternative example:
  //
  //   --ysql_pg_conf="shared_preload_libraries='$LIB1,$LIB2,$LIB3'"
  // where any of the libs could be undefined.
  new_items.erase(
    std::remove_if(new_items.begin(),
      new_items.end(),
      [](const std::string& s){return s.empty();}),
      new_items.end());
  defaults->insert(defaults->end(), new_items.begin(), new_items.end());
}

Status ReadCSVValues(const string& csv, vector<string>* lines) {
  // Function reads CSV string in the following format:
  // - fields are divided with comma (,)
  // - fields with comma (,) or double-quote (") are quoted with double-quote (")
  // - pair of double-quote ("") in quoted field represents single double-quote (")
  //
  // Examples:
  // 1,"two, 2","three ""3""", four , -> ['1', 'two, 2', 'three "3"', ' four ', '']
  // 1,"two                           -> Malformed CSV (quoted field 'two' is not closed)
  // 1, "two"                         -> Malformed CSV (quoted field 'two' has leading spaces)
  // 1,two "2"                        -> Malformed CSV (field with " must be quoted)
  // 1,"tw"o"                         -> Malformed CSV (no separator after quoted field 'tw')

  const std::regex exp(R"(^(?:([^,"]+)|(?:"((?:[^"]|(?:""))*)\"))(?:(?:,)|(?:$)))");
  auto i = csv.begin();
  const auto end = csv.end();
  std::smatch match;
  while (i != end && std::regex_search(i, end, match, exp)) {
    // Replace pair of double-quote ("") with single double-quote (") in quoted field.
    if (match[2].length() > 0) {
      lines->emplace_back(match[2].first, match[2].second);
      boost::algorithm::replace_all(lines->back(), "\"\"", "\"");
    } else {
      lines->emplace_back(match[1].first, match[1].second);
    }
    i += match.length();
  }
  SCHECK(i == end, InvalidArgument, Format("Malformed CSV '$0'", csv));
  if (!csv.empty() && csv.back() == ',') {
    lines->emplace_back();
  }
  return Status::OK();
}

namespace {
// Append any Pg gFlag with non default value, or non-promoted AutoFlag
void AppendPgGFlags(vector<string>* lines) {
  vector<CommandLineFlagInfo> flags;
  GetAllFlags(&flags);

  for (const CommandLineFlagInfo& flag : flags) {
    std::unordered_set<FlagTag> tags;
    GetFlagTags(flag.name, &tags);
    if (!tags.contains(FlagTag::kPg)) {
      continue;
    }

    // Skip flags that do not have a custom override
    if (flag.is_default) {
      if (!tags.contains(FlagTag::kAuto)) {
        continue;
      }

      // AutoFlags in not-promoted state will be set to their initial value.
      // In the promoted state they will be set to their target value. (guc default)
      // We only need to override when AutoFlags is non-promoted.
      auto* desc = GetAutoFlagDescription(flag.name);
      CHECK_NOTNULL(desc);
      if (IsFlagPromoted(flag, *desc)) {
        continue;
      }
    }

    const string pg_flag_prefix = "ysql_";
    if (!flag.name.starts_with(pg_flag_prefix)) {
      LOG(DFATAL) << "Flags with Pg Flag tag should have 'ysql_' prefix. Flag_name: " << flag.name;
      continue;
    }

    string pg_variable_name = flag.name.substr(pg_flag_prefix.length());
    lines->push_back(Format("$0=$1", pg_variable_name, flag.current_value));
  }
}
}  // namespace

Result<string> WritePostgresConfig(const PgProcessConf& conf) {
  // First add default configuration created by local initdb.
  string default_conf_path = JoinPathSegments(conf.data_dir, "postgresql.conf");
  std::ifstream conf_file;
  conf_file.open(default_conf_path, std::ios_base::in);
  if (!conf_file) {
    return STATUS_FORMAT(
        IOError,
        "Failed to read default postgres configuration '%s': errno=$0: $1",
        default_conf_path,
        errno,
        ErrnoToString(errno));
  }

  // Gather the default extensions:
  vector<string> metricsLibs;
  if (FLAGS_pg_stat_statements_enabled) {
    metricsLibs.push_back("pg_stat_statements");
  }
  metricsLibs.push_back("yb_pg_metrics");
  metricsLibs.push_back("pgaudit");
  metricsLibs.push_back("pg_hint_plan");

  vector<string> lines;
  string line;
  while (std::getline(conf_file, line)) {
    lines.push_back(line);
  }
  conf_file.close();

  vector<string> user_configs;
  if (!FLAGS_ysql_pg_conf_csv.empty()) {
    RETURN_NOT_OK(ReadCSVValues(FLAGS_ysql_pg_conf_csv, &user_configs));
  } else if (!FLAGS_ysql_pg_conf.empty()) {
    ReadCommaSeparatedValues(FLAGS_ysql_pg_conf, &user_configs);
  }

  // If the user has given any shared_preload_libraries, merge them in.
  for (string &value : user_configs) {
    if (boost::starts_with(value, "shared_preload_libraries")) {
      MergeSharedPreloadLibraries(value, &metricsLibs);
    } else {
      lines.push_back(value);
    }
  }

  // Add shared_preload_libraries to the ysql_pg.conf.
  lines.push_back(Format("shared_preload_libraries='$0'", boost::join(metricsLibs, ",")));

  if (conf.enable_tls) {
    lines.push_back("ssl=on");
    lines.push_back(Format("ssl_cert_file='$0/node.$1.crt'",
                           conf.certs_for_client_dir,
                           conf.cert_base_name));
    lines.push_back(Format("ssl_key_file='$0/node.$1.key'",
                           conf.certs_for_client_dir,
                           conf.cert_base_name));
    lines.push_back(Format("ssl_ca_file='$0/ca.crt'", conf.certs_for_client_dir));
  }

  // Finally add gFlags.
  // If the file contains multiple entries for the same parameter, all but the last one are
  // ignored. If there are duplicates in FLAGS_ysql_pg_conf_csv then we want the values specified
  // via the gFlag to take precedence.
  AppendPgGFlags(&lines);

  string conf_path = JoinPathSegments(conf.data_dir, "ysql_pg.conf");
  RETURN_NOT_OK(WriteConfigFile(conf_path, lines));
  return "config_file=" + conf_path;
}

Result<string> WritePgHbaConfig(const PgProcessConf& conf) {
  vector<string> lines;

  // Add the user-defined custom configuration lines if any.
  // Put this first so that it can be used to override the auto-generated config below.
  if (!FLAGS_ysql_hba_conf_csv.empty()) {
    RETURN_NOT_OK(ReadCSVValues(FLAGS_ysql_hba_conf_csv, &lines));
  } else if (!FLAGS_ysql_hba_conf.empty()) {
    ReadCommaSeparatedValues(FLAGS_ysql_hba_conf, &lines);
  }

  // Add auto-generated config for the enable auth and enable_tls flags.
  if (FLAGS_ysql_enable_auth || conf.enable_tls) {
    const auto host_type =  conf.enable_tls ? "hostssl" : "host";
    const auto auth_method = FLAGS_ysql_enable_auth ? "md5" : "trust";
    lines.push_back(Format("$0 all all all $1", host_type, auth_method));
  }

  // Enforce a default hba configuration so users don't lock themselves out.
  if (lines.empty()) {
    LOG(WARNING) << "No hba configuration lines found, defaulting to trust all configuration.";
    lines.push_back("host all all all trust");
  }

  // Add comments to the hba config file noting the internally hardcoded config line.
  lines.insert(lines.begin(), {
      "# Internal configuration:",
      "# local all postgres yb-tserver-key",
  });

  const auto conf_path = JoinPathSegments(conf.data_dir, "ysql_hba.conf");
  RETURN_NOT_OK(WriteConfigFile(conf_path, lines));
  return "hba_file=" + conf_path;
}

Result<vector<string>> WritePgConfigFiles(const PgProcessConf& conf) {
  vector<string> args;
  args.push_back("-c");
  args.push_back(VERIFY_RESULT_PREPEND(WritePostgresConfig(conf),
      "Failed to write ysql pg configuration: "));
  args.push_back("-c");
  args.push_back(VERIFY_RESULT_PREPEND(WritePgHbaConfig(conf),
      "Failed to write ysql hba configuration: "));
  return args;
}

}  // namespace

string GetPostgresInstallRoot() {
  return JoinPathSegments(yb::env_util::GetRootDir("postgres"), "postgres");
}

Result<PgProcessConf> PgProcessConf::CreateValidateAndRunInitDb(
    const std::string& bind_addresses,
    const std::string& data_dir,
    const int tserver_shm_fd) {
  PgProcessConf conf;
  if (!bind_addresses.empty()) {
    auto pg_host_port = VERIFY_RESULT(HostPort::FromString(
        bind_addresses, PgProcessConf::kDefaultPort));
    conf.listen_addresses = pg_host_port.host();
    conf.pg_port = pg_host_port.port();
  }
  conf.data_dir = data_dir;
  conf.tserver_shm_fd = tserver_shm_fd;
  PgWrapper pg_wrapper(conf);
  RETURN_NOT_OK(pg_wrapper.PreflightCheck());
  RETURN_NOT_OK(pg_wrapper.InitDbLocalOnlyIfNeeded());
  return conf;
}

// ------------------------------------------------------------------------------------------------
// PgWrapper: managing one instance of a PostgreSQL child process
// ------------------------------------------------------------------------------------------------

PgWrapper::PgWrapper(PgProcessConf conf)
    : conf_(std::move(conf)) {
}

Status PgWrapper::PreflightCheck() {
  RETURN_NOT_OK(CheckExecutableValid(GetPostgresExecutablePath()));
  RETURN_NOT_OK(CheckExecutableValid(GetInitDbExecutablePath()));
  return Status::OK();
}

Status PgWrapper::Start() {
  auto postgres_executable = GetPostgresExecutablePath();
  RETURN_NOT_OK(CheckExecutableValid(postgres_executable));
  vector<string> argv {
    postgres_executable,
    "-D", conf_.data_dir,
    "-p", std::to_string(conf_.pg_port),
    "-h", conf_.listen_addresses,
  };

  bool log_to_file = !FLAGS_logtostderr && !FLAGS_log_dir.empty() && !conf_.force_disable_log_file;
  VLOG(1) << "Deciding whether the child postgres process should to file: "
          << EXPR_VALUE_FOR_LOG(FLAGS_logtostderr) << ", "
          << EXPR_VALUE_FOR_LOG(FLAGS_log_dir.empty()) << ", "
          << EXPR_VALUE_FOR_LOG(conf_.force_disable_log_file) << ": "
          << EXPR_VALUE_FOR_LOG(log_to_file);

  // Configure UNIX domain socket for index backfill tserver-postgres communication and for
  // Yugabyte Platform backups.
  argv.push_back("-k");
  const std::string& socket_dir = PgDeriveSocketDir(
      HostPort(conf_.listen_addresses, conf_.pg_port));
  RETURN_NOT_OK(Env::Default()->CreateDirs(socket_dir));
  argv.push_back(socket_dir);

  // Also tighten permissions on the socket.
  argv.push_back("-c");
  argv.push_back("unix_socket_permissions=0700");

  if (log_to_file) {
    argv.push_back("-c");
    argv.push_back("logging_collector=on");
    // FLAGS_log_dir should already be set by tserver during startup.
    argv.push_back("-c");
    argv.push_back("log_directory=" + FLAGS_log_dir);
  }

  argv.push_back("-c");
  argv.push_back("yb_pg_metrics.node_name=" + FLAGS_metric_node_name);
  argv.push_back("-c");
  argv.push_back("yb_pg_metrics.port=" + std::to_string(FLAGS_pgsql_proxy_webserver_port));

  auto config_file_args = CHECK_RESULT(WritePgConfigFiles(conf_));
  argv.insert(argv.end(), config_file_args.begin(), config_file_args.end());

  if (FLAGS_pg_verbose_error_log) {
    argv.push_back("-c");
    argv.push_back("log_error_verbosity=VERBOSE");
  }

  pg_proc_.emplace(postgres_executable, argv);
  vector<string> ld_library_path {
    GetPostgresLibPath(),
    GetPostgresThirdPartyLibPath()
  };
  pg_proc_->SetEnv("LD_LIBRARY_PATH", boost::join(ld_library_path, ":"));
  pg_proc_->ShareParentStderr();
  pg_proc_->ShareParentStdout();
  pg_proc_->SetEnv("FLAGS_yb_pg_terminate_child_backend",
                    FLAGS_yb_pg_terminate_child_backend ? "true" : "false");
  pg_proc_->SetEnv("FLAGS_yb_backend_oom_score_adj", FLAGS_yb_backend_oom_score_adj);

  // See YBSetParentDeathSignal in pg_yb_utils.c for how this is used.
  pg_proc_->SetEnv("YB_PG_PDEATHSIG", Format("$0", SIGINT));
  pg_proc_->InheritNonstandardFd(conf_.tserver_shm_fd);
  SetCommonEnv(&*pg_proc_, /* yb_enabled */ true);

  pg_proc_->SetEnv("FLAGS_mem_tracker_tcmalloc_gc_release_bytes",
                FLAGS_pg_mem_tracker_tcmalloc_gc_release_bytes);
  pg_proc_->SetEnv("FLAGS_mem_tracker_update_consumption_interval_us",
                FLAGS_pg_mem_tracker_update_consumption_interval_us);

  RETURN_NOT_OK(pg_proc_->Start());
  if (!FLAGS_postmaster_cgroup.empty()) {
    std::string path = FLAGS_postmaster_cgroup + "/cgroup.procs";
    pg_proc_->AddPIDToCGroup(path, pg_proc_->pid());
  }
  LOG(INFO) << "PostgreSQL server running as pid " << pg_proc_->pid();
  return Status::OK();
}

Status PgWrapper::ReloadConfig() {
  return pg_proc_->Kill(SIGHUP);
}

Status PgWrapper::UpdateAndReloadConfig() {
  VERIFY_RESULT(WritePostgresConfig(conf_));
  return ReloadConfig();
}

void PgWrapper::Kill() {
  int signal = SIGINT;
  // TODO(fizaa): Use SIGQUIT in asan build until GH #15168 is fixed.
#ifdef ADDRESS_SANITIZER
  signal = SIGQUIT;
#endif
  WARN_NOT_OK(pg_proc_->Kill(signal), "Kill PostgreSQL server failed");
}

Status PgWrapper::InitDb(bool yb_enabled) {
  const string initdb_program_path = GetInitDbExecutablePath();
  RETURN_NOT_OK(CheckExecutableValid(initdb_program_path));
  if (!Env::Default()->FileExists(initdb_program_path)) {
    return STATUS_FORMAT(IOError, "initdb not found at: $0", initdb_program_path);
  }

  vector<string> initdb_args { initdb_program_path, "-D", conf_.data_dir, "-U", "postgres" };
  LOG(INFO) << "Launching initdb: " << AsString(initdb_args);

  Subprocess initdb_subprocess(initdb_program_path, initdb_args);
  initdb_subprocess.InheritNonstandardFd(conf_.tserver_shm_fd);
  SetCommonEnv(&initdb_subprocess, yb_enabled);
  int status = 0;
  RETURN_NOT_OK(initdb_subprocess.Start());
  RETURN_NOT_OK(initdb_subprocess.Wait(&status));
  if (status != 0) {
    SCHECK(WIFEXITED(status), InternalError,
           Format("$0 did not exit normally", initdb_program_path));
    return STATUS_FORMAT(RuntimeError, "$0 failed with exit code $1",
                         initdb_program_path,
                         WEXITSTATUS(status));
  }

  LOG(INFO) << "initdb completed successfully. Database initialized at " << conf_.data_dir;
  return Status::OK();
}

Status PgWrapper::InitDbLocalOnlyIfNeeded() {
  if (Env::Default()->FileExists(conf_.data_dir)) {
    LOG(INFO) << "Data directory " << conf_.data_dir << " already exists, skipping initdb";
    return Status::OK();
  }
  // Do not communicate with the YugaByte cluster at all. This function is only concerned with
  // setting up the local PostgreSQL data directory on this tablet server.
  return InitDb(/* yb_enabled */ false);
}

Result<int> PgWrapper::Wait() {
  if (!pg_proc_) {
    return STATUS(IllegalState,
                  "PostgreSQL child process has not been started, cannot wait for it to exit");
  }
  return pg_proc_->Wait();
}

Status PgWrapper::InitDbForYSQL(
    const string& master_addresses, const string& tmp_dir_base,
    int tserver_shm_fd) {
  LOG(INFO) << "Running initdb to initialize YSQL cluster with master addresses "
            << master_addresses;
  PgProcessConf conf;
  conf.master_addresses = master_addresses;
  conf.pg_port = 0;  // We should not use this port.
  std::mt19937 rng{std::random_device()()};
  conf.data_dir = Format("$0/tmp_pg_data_$1", tmp_dir_base, rng());
  conf.tserver_shm_fd = tserver_shm_fd;
  auto se = ScopeExit([&conf] {
    auto is_dir = Env::Default()->IsDirectory(conf.data_dir);
    if (is_dir.ok()) {
      if (is_dir.get()) {
        Status del_status = Env::Default()->DeleteRecursively(conf.data_dir);
        if (!del_status.ok()) {
          LOG(WARNING) << "Failed to delete directory " << conf.data_dir;
        }
      }
    } else if (!is_dir.status().IsNotFound()) {
      LOG(WARNING) << "Failed to check directory existence for " << conf.data_dir << ": "
                   << is_dir.status();
    }
  });
  PgWrapper pg_wrapper(conf);
  auto start_time = std::chrono::steady_clock::now();
  Status initdb_status = pg_wrapper.InitDb(/* yb_enabled */ true);
  auto elapsed_time = std::chrono::steady_clock::now() - start_time;
  LOG(INFO)
      << "initdb took "
      << std::chrono::duration_cast<std::chrono::milliseconds>(elapsed_time).count() << " ms";
  if (!initdb_status.ok()) {
    LOG(ERROR) << "initdb failed: " << initdb_status;
  }
  return initdb_status;
}

string PgWrapper::GetPostgresExecutablePath() {
  return JoinPathSegments(GetPostgresInstallRoot(), "bin", "postgres");
}

string PgWrapper::GetPostgresLibPath() {
  return JoinPathSegments(GetPostgresInstallRoot(), "lib");
}

string PgWrapper::GetPostgresThirdPartyLibPath() {
  return JoinPathSegments(GetPostgresInstallRoot(), "..", "lib", "yb-thirdparty");
}

string PgWrapper::GetInitDbExecutablePath() {
  return JoinPathSegments(GetPostgresInstallRoot(), "bin", "initdb");
}

Status PgWrapper::CheckExecutableValid(const std::string& executable_path) {
  if (VERIFY_RESULT(Env::Default()->IsExecutableFile(executable_path))) {
    return Status::OK();
  }
  return STATUS_FORMAT(NotFound, "Not an executable file: $0", executable_path);
}

void PgWrapper::SetCommonEnv(Subprocess* proc, bool yb_enabled) {
  // Used to resolve relative paths during YB init within PG code.
  // Needed because PG changes its current working dir to a data dir.
  char cwd[PATH_MAX];
  CHECK(getcwd(cwd, sizeof(cwd)) != nullptr);
  proc->SetEnv("YB_WORKING_DIR", cwd);
  // A temporary workaround for a failure to look up a user name by uid in an LDAP environment.
  proc->SetEnv("YB_PG_FALLBACK_SYSTEM_USER_NAME", "postgres");
  proc->SetEnv("YB_PG_ALLOW_RUNNING_AS_ANY_USER", "1");
  proc->SetEnv("FLAGS_pggate_tserver_shm_fd", std::to_string(conf_.tserver_shm_fd));
#ifdef OS_MACOSX
  // Postmaster with NLS support fails to start on Mac unless LC_ALL is properly set
  if (getenv("LC_ALL") == nullptr) {
    proc->SetEnv("LC_ALL", "en_US.UTF-8");
  }
#endif
  if (yb_enabled) {
    proc->SetEnv("YB_ENABLED_IN_POSTGRES", "1");
    proc->SetEnv("FLAGS_pggate_master_addresses", conf_.master_addresses);
    // Postgres process can't compute default certs dir by itself
    // as it knows nothing about t-server's root data directory.
    // Solution is to specify it explicitly.
    proc->SetEnv("FLAGS_certs_dir", conf_.certs_dir);
    proc->SetEnv("FLAGS_certs_for_client_dir", conf_.certs_for_client_dir);

    proc->SetEnv("YB_PG_TRANSACTIONS_ENABLED", FLAGS_pg_transactions_enabled ? "1" : "0");

#ifdef ADDRESS_SANITIZER
    // Disable reporting signal-unsafe behavior for PostgreSQL because it does a lot of work in
    // signal handlers on shutdown.

    const char* asan_options = getenv("ASAN_OPTIONS");
    proc->SetEnv(
        "ASAN_OPTIONS",
        std::string(asan_options ? asan_options : "") + " report_signal_unsafe=0");
#endif

    // Pass non-default flags to the child process using FLAGS_... environment variables.
    static const std::vector<string> explicit_flags{"pggate_master_addresses",
                                                    "pggate_tserver_shm_fd",
                                                    "certs_dir",
                                                    "certs_for_client_dir",
                                                    "mem_tracker_tcmalloc_gc_release_bytes",
                                                    "mem_tracker_update_consumption_interval_us"};
    std::vector<google::CommandLineFlagInfo> flag_infos;
    google::GetAllFlags(&flag_infos);
    for (const auto& flag_info : flag_infos) {
      // Skip the flags that we set explicitly using conf_ above.
      if (!flag_info.is_default &&
          std::find(explicit_flags.begin(),
                    explicit_flags.end(),
                    flag_info.name) == explicit_flags.end()) {
        proc->SetEnv("FLAGS_" + flag_info.name, flag_info.current_value);
      }
    }
  } else {
    proc->SetEnv("YB_PG_LOCAL_NODE_INITDB", "1");
  }
}

// ------------------------------------------------------------------------------------------------
// PgSupervisor: monitoring a PostgreSQL child process and restarting if needed
// ------------------------------------------------------------------------------------------------

PgSupervisor::PgSupervisor(PgProcessConf conf, tserver::TabletServerIf* tserver)
    : conf_(std::move(conf)) {
  if (tserver) {
    tserver->RegisterCertificateReloader(std::bind(&PgSupervisor::ReloadConfig, this));
  }
}

PgSupervisor::~PgSupervisor() {
  std::lock_guard<std::mutex> lock(mtx_);
  DeregisterPgFlagChangeNotifications();
}

Status PgSupervisor::Start() {
  std::lock_guard<std::mutex> lock(mtx_);
  RETURN_NOT_OK(ExpectStateUnlocked(PgProcessState::kNotStarted));
  RETURN_NOT_OK(CleanupOldServerUnlocked());
  RETURN_NOT_OK(RegisterPgFlagChangeNotifications());
  LOG(INFO) << "Starting PostgreSQL server";
  RETURN_NOT_OK(StartServerUnlocked());

  Status status = Thread::Create(
      "pg_supervisor", "pg_supervisor", &PgSupervisor::RunThread, this, &supervisor_thread_);
  if (!status.ok()) {
    supervisor_thread_.reset();
    return status;
  }

  state_ = PgProcessState::kRunning;

  return Status::OK();
}

Status PgSupervisor::CleanupOldServerUnlocked() {
  std::string postmaster_pid_filename = JoinPathSegments(conf_.data_dir, "postmaster.pid");
  if (Env::Default()->FileExists(postmaster_pid_filename)) {
    std::ifstream postmaster_pid_file;
    postmaster_pid_file.open(postmaster_pid_filename, std::ios_base::in);
    pid_t postgres_pid = 0;

    if (!postmaster_pid_file.eof()) {
      postmaster_pid_file >> postgres_pid;
    }

    if (!postmaster_pid_file.good() || postgres_pid == 0) {
      LOG(ERROR) << strings::Substitute("Error reading postgres process ID from file $0. $1 $2",
          postmaster_pid_filename, ErrnoToString(errno), errno);
    } else {
      LOG(WARNING) << "Killing older postgres process: " << postgres_pid;
      // If process does not exist, system may return "process does not exist" or
      // "operation not permitted" error. Ignore those errors.
      postmaster_pid_file.close();
      bool postgres_found = true;
      string cmdline = "";
#ifdef __linux__
      string cmd_filename = "/proc/" + std::to_string(postgres_pid) + "/cmdline";
      std::ifstream postmaster_cmd_file;
      postmaster_cmd_file.open(cmd_filename, std::ios_base::in);
      if (postmaster_cmd_file.good()) {
        postmaster_cmd_file >> cmdline;
        postgres_found = cmdline.find("/postgres") != std::string::npos;
        postmaster_cmd_file.close();
      }
#endif
      if (postgres_found) {
        if (kill(postgres_pid, SIGKILL) != 0 && errno != ESRCH && errno != EPERM) {
          return STATUS(RuntimeError, "Unable to kill", Errno(errno));
        }
      } else {
        LOG(WARNING) << "Didn't find postgres in " << cmdline;
      }
    }
    WARN_NOT_OK(Env::Default()->DeleteFile(postmaster_pid_filename),
                "Failed to remove postmaster pid file");
  }
  return Status::OK();
}

PgProcessState PgSupervisor::GetState() {
  std::lock_guard<std::mutex> lock(mtx_);
  return state_;
}

Status PgSupervisor::ExpectStateUnlocked(PgProcessState expected_state) {
  if (state_ != expected_state) {
    return STATUS_FORMAT(
        IllegalState, "Expected PostgreSQL server state to be $0, got $1", expected_state, state_);
  }
  return Status::OK();
}

Status PgSupervisor::StartServerUnlocked() {
  if (pg_wrapper_) {
    return STATUS(IllegalState, "Expecting pg_wrapper_ to not be set");
  }
  pg_wrapper_.emplace(conf_);
  auto start_status = pg_wrapper_->Start();
  if (!start_status.ok()) {
    pg_wrapper_.reset();
    return start_status;
  }
  return Status::OK();
}

void PgSupervisor::RunThread() {
  while (true) {
    Result<int> wait_result = pg_wrapper_->Wait();
    if (wait_result.ok()) {
      int ret_code = *wait_result;
      if (ret_code == 0) {
        LOG(INFO) << "PostgreSQL server exited normally";
      } else {
        LOG(WARNING) << "PostgreSQL server exited with code " << ret_code;
      }
      pg_wrapper_.reset();
    } else {
      // TODO: a better way to handle this error.
      LOG(WARNING) << "Failed when waiting for PostgreSQL server to exit: "
                   << wait_result.status() << ", waiting a bit";
      std::this_thread::sleep_for(1s);
      continue;
    }

    {
      std::lock_guard<std::mutex> lock(mtx_);
      if (state_ == PgProcessState::kStopping) {
        break;
      }
      LOG(INFO) << "Restarting PostgreSQL server";
      Status start_status = StartServerUnlocked();
      if (!start_status.ok()) {
        // TODO: a better way to handle this error.
        LOG(WARNING) << "Failed trying to start PostgreSQL server: "
                     << start_status << ", waiting a bit";
        std::this_thread::sleep_for(1s);
      }
    }
  }
}

void PgSupervisor::Stop() {
  {
    std::lock_guard<std::mutex> lock(mtx_);
    state_ = PgProcessState::kStopping;
    DeregisterPgFlagChangeNotifications();

    if (pg_wrapper_) {
      pg_wrapper_->Kill();
    }
  }
  supervisor_thread_->Join();
}

Status PgSupervisor::ReloadConfig() {
  std::lock_guard<std::mutex> lock(mtx_);
  if (pg_wrapper_) {
    return pg_wrapper_->ReloadConfig();
  }
  return Status::OK();
}

Status PgSupervisor::UpdateAndReloadConfig() {
  std::lock_guard<std::mutex> lock(mtx_);
  if (pg_wrapper_) {
    return pg_wrapper_->UpdateAndReloadConfig();
  }
  return Status::OK();
}

Status PgSupervisor::RegisterReloadPgConfigCallback(const void* flag_ptr) {
  // DeRegisterForPgFlagChangeNotifications is called before flag_callbacks_ is destroyed, so its
  // safe to bind to this.
  flag_callbacks_.emplace_back(VERIFY_RESULT(RegisterFlagUpdateCallback(
      flag_ptr, "ReloadPgConfig", std::bind(&PgSupervisor::UpdateAndReloadConfig, this))));

  return Status::OK();
}

Status PgSupervisor::RegisterPgFlagChangeNotifications() {
  DeregisterPgFlagChangeNotifications();

  vector<CommandLineFlagInfo> flags;
  GetAllFlags(&flags);
  for (const CommandLineFlagInfo& flag : flags) {
    std::unordered_set<FlagTag> tags;
    GetFlagTags(flag.name, &tags);
    if (tags.contains(FlagTag::kPg)) {
      RETURN_NOT_OK(RegisterReloadPgConfigCallback(flag.flag_ptr));
    }
  }

  RETURN_NOT_OK(RegisterReloadPgConfigCallback(&FLAGS_ysql_pg_conf_csv));

  return Status::OK();
}

void PgSupervisor::DeregisterPgFlagChangeNotifications() {
  for (auto& callback : flag_callbacks_) {
    callback.Deregister();
  }
  flag_callbacks_.clear();
}
}  // namespace pgwrapper
}  // namespace yb
