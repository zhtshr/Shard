
#include "util/json_config.h"
#include "smart/benchmark.h"
#include "shard.h"

int main(int argc, char **argv) {
    using namespace sds;
    using namespace sds::datastructure;
    const char *path = ROOT_DIR "/config/datastructure.json";
    if (getenv("APP_CONFIG_PATH")) {
        path = getenv("APP_CONFIG_PATH");
    }
    JsonConfig config = JsonConfig::load_file(path);
    setenv("REPORT_LATENCY", "1", 0);
    setenv("COMPUTE_NODES" , "1", 0);

    if (config.get("memory_servers").size() > 1) {
        BenchmarkRunner<RemoteShardMultiShard> runner(config, argc, argv);
        if (runner.spawn()) {
            exit(EXIT_FAILURE);
        }
    } else {
        BenchmarkRunner<RemoteShard> runner(config, argc, argv);
        if (runner.spawn()) {
            exit(EXIT_FAILURE);
        }
    }
    return 0;
}