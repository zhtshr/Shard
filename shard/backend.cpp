
#include "util/json_config.h"
#include "smart/target.h"

#include "shard.h"

using namespace sds;

void scan_move(int node_id) {
    const char *path = ROOT_DIR "/config/datastructure.json";
    JsonConfig config = JsonConfig::load_file(path);
    // BindCore((int)config.get("nr_threads").get_uint64());

    Initiator *node = new Initiator();
    if (!node) {
        exit(EXIT_FAILURE);
    }

    RemoteShard *index = new RemoteShard(config, node, node_id);
    SDS_INFO("%d %d",GetThreadID(),GetTaskID());
    while (true) {
        int resize_flag = index->read_hash_table();
        if (resize_flag) {
            SDS_INFO("begin to move bins");
            index->move_bins();
        }
        sleep(0.1);
    }
    // auto entry = config.get("memory_servers").get(node_id);
    // std::string domain_name = entry.get("hostname").get_str();
    // uint16_t tcp_port = (uint16_t) entry.get("port").get_int64();
    // if (node->connect(node_id, domain_name.c_str(), tcp_port, 1)) {
    //     exit(EXIT_FAILURE);
    // }

    // GlobalAddress ht_addr;
    // if (node->get_root_entry(node_id, 0, ht_addr.raw)) {
    //     exit(EXIT_FAILURE);
    // }
    // SDS_INFO("%lx",ht_addr.raw);
}

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
    RemoteShard::Setup(config, target);
    SDS_INFO("Press C to stop the memory node daemon.");
    target.start(tcp_port);
    // scan_move((int) config.get("node_id").get_int64());
    // std::thread bg_worker(scan_move, (int) config.get("node_id").get_int64());
    while (getchar() != 'c') { sleep(1); }
    // bg_worker.join();
    target.stop();
    return 0;
}
