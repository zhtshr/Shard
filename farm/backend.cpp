
#include "util/json_config.h"
#include "smart/target.h"

#include "hopscotch.h"

using namespace sds;

int main(int argc, char **argv) {
    WritePidFile();
    const char *path = ROOT_DIR "/config/backend.json";
    if (argc == 2) {
        path = argv[1];
    }
    JsonConfig config = JsonConfig::load_file(path);
    BindCore((int) config.get("nic_numa_node").get_int64());
    std::string dev_dax_path = ""; // config.get("dev_dax_path").get_str();
    size_t capacity = config.get("capacity").get_uint64() * kMegaBytes;
    uint16_t tcp_port = (uint16_t) config.get("tcp_port").get_int64();
    Target target;
    void *mmap_addr = mapping_memory(dev_dax_path, capacity);
    int rc = target.register_main_memory(mmap_addr, capacity);
    assert(!rc);
    RemoteHopscotch::Setup(config, target);
    SDS_INFO("Press C to stop the memory node daemon.");
    target.start(tcp_port);
    while (getchar() != 'c') { sleep(1); }
    // bg_worker.join();
    target.stop();
    return 0;
}
