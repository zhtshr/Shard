
#include <iostream>
#include <cassert>
#include <unistd.h>
#include <sys/time.h>
#include <sys/mman.h>
#include <immintrin.h>
#include <atomic>
#include <random>

#include "smart/thread.h"

#include "smart/initiator.h"
#include "smart/target.h"

using namespace sds;

static const size_t MEM_POOL_SIZE = (1ull << 30);

void run_server(uint16_t port) {
    Target target;
    char *local_addr = (char *) mmap_huge_page(MEM_POOL_SIZE);
    memset(local_addr, 0, MEM_POOL_SIZE);
    int rc = target.register_main_memory(local_addr, MEM_POOL_SIZE);
    assert(!rc);
    rc = target.start(port);
    assert(!rc);
    SDS_INFO("server starts, press c to exit");
    while (getchar() != 'c');
    target.stop();
}

size_t connections = 1;
size_t nr_nodes = 1;
int nr_threads = 1;
size_t block_size = 64;
int qp_num;
std::string dump_file_path;
std::string dump_prefix;
std::string type;

std::atomic<uint64_t> total_attempts(0);
volatile int stop_signal = 0;
pthread_barrier_t barrier;
Initiator *node[32];

void *test_thread_func(void *arg) {
    int thread_id = (int) (uintptr_t) arg;
    auto ctx = node[thread_id % nr_nodes];
    BindCore(thread_id);
    size_t kSegmentSize = MEM_POOL_SIZE / nr_threads;
    kSegmentSize &= ~4095ull;
    size_t align_size = block_size < 64 ? 64 : block_size;
    char *buf = (char *) ctx->alloc_cache(align_size + 8);
    uint64_t bucket_num = block_size / 64;
    pthread_barrier_wait(&barrier);
    std::mt19937 rnd;
    std::uniform_int_distribution<uint64_t> dist(0, 32 * bucket_num - 1);
    if (thread_id == 0) {
        int tot = 0, cnt = 0;
        int i = 0;
        while (!stop_signal && i < bucket_num) {
            tot+=3;
            uint64_t offset = 0;
            GlobalAddress remote_addr(0, i * 64);
            int rc = ctx->read(buf, remote_addr, 64, Initiator::Option::Sync);
            assert(!rc);
            uint64_t *slots = (uint64_t *)buf;
            for (int j = 0; j < 3; j++) {
                for (int k = 0; k < 8; k++) {
                    if (slots[k] == 0) {
                        rc = ctx->compare_and_swap(buf + 64, remote_addr + k * 8, slots[k], 1, Initiator::Option::Sync);
                        assert(!rc);
                        if (*(uint64_t*)(buf + 64) == 0) {
                            cnt++;
                            break;
                        }
                    }
                }
            }
            i++;
        }
        SDS_INFO("%d %d", tot, cnt);
    } else {
        while (!stop_signal) {
            uint64_t idx = dist(rnd);
            if (idx < bucket_num) {
                GlobalAddress remote_addr(0, idx * 64);
                int rc = ctx->read(buf, remote_addr, 64, Initiator::Option::Sync);
                assert(!rc);
                uint64_t *slots = (uint64_t *)buf;
                for (int k = 0; k < 8; k++) {
                    if (slots[k] == 0) {
                        rc = ctx->compare_and_swap(buf + 64, remote_addr + k * 8, slots[k], 1, Initiator::Option::Sync);
                        assert(!rc);
                        if (*(uint64_t*)(buf + 64) == 0) {
                            break;
                        }
                    }
                }
            }
            else {
                idx %= 2*bucket_num;
                GlobalAddress remote_addr(0, idx * 64);
                int rc = ctx->read(buf, remote_addr, 64, Initiator::Option::Sync);
                assert(!rc);
                rc = ctx->read(buf, remote_addr, 64, Initiator::Option::Sync);
                assert(!rc);
            }
        }
    } 
    pthread_barrier_wait(&barrier);
    return NULL;
}

double connect_time = 0.0;

void run_client(const std::vector<std::string> &server_list, uint16_t port) {
    struct timeval start_tv, end_tv;
    pthread_t tid[kMaxThreads];
    double elapsed_time;
    int qp_count = nr_threads;
    if (qp_num > 0) {
        qp_count = qp_num;
    }
    if (qp_num < 0) {
        qp_count = (nr_threads - qp_num - 1) / -qp_num;
    }
    gettimeofday(&start_tv, NULL);
    for (int i = 0; i < nr_nodes; ++i) {
        node[i] = new Initiator();
        node[i]->disable_inline_write();
        for (int j = 0; j < connections; ++j) {
            int rc = node[i]->connect(j, server_list[j % server_list.size()].c_str(),
                                      port, qp_count);
            assert(!rc);
        }
    }
    gettimeofday(&end_tv, NULL);
    connect_time = (end_tv.tv_sec - start_tv.tv_sec) * 1000.0 +
                   (end_tv.tv_usec - start_tv.tv_usec) / 1000.0;
    pthread_barrier_init(&barrier, NULL, nr_threads + 1);
    for (long i = 0; i < nr_threads; ++i) {
        pthread_create(&tid[i], NULL, test_thread_func, (void *) i);
    }
    pthread_barrier_wait(&barrier);
    gettimeofday(&start_tv, NULL);
    sleep(5);
    stop_signal = 1;
    pthread_barrier_wait(&barrier);
    gettimeofday(&end_tv, NULL);
    for (int i = 0; i < nr_threads; ++i) {
        pthread_join(tid[i], NULL);
    }
    pthread_barrier_destroy(&barrier);
    elapsed_time = (end_tv.tv_sec - start_tv.tv_sec) * 1.0 +
                   (end_tv.tv_usec - start_tv.tv_usec) / 1000000.0;
    // report(elapsed_time);

    for (int i = 0; i < nr_nodes; ++i) {
        for (int j = 0; j < connections; ++j) {
            node[i]->disconnect(j);
        }
        delete node[i];
    }
}

int main(int argc, char **argv) {
    const char *env_path = getenv("TEST_RDMA_CONF");
    JsonConfig config = JsonConfig::load_file(env_path ? env_path : ROOT_DIR "/config/test_rdma.json");
    qp_num = (int) config.get("qp_num").get_int64();
    if (getenv("QP_NUM")) {
        qp_num = atoi(getenv("QP_NUM"));
    }
    int port = (int) config.get("port").get_int64();
    dump_file_path = config.get("dump_file_path").get_str();
    if (getenv("DUMP_FILE_PATH")) {
        dump_file_path = getenv("DUMP_FILE_PATH");
    }
    type = config.get("type").get_str();
    if (getenv("TYPE")) {
        type = getenv("TYPE");
    }
    if (getenv("DUMP_PREFIX")) {
        dump_prefix = std::string(getenv("DUMP_PREFIX"));
    } else {
        dump_prefix = "rdma-" + type;
    }
    BindCore(0);
    if (argc == 1) {
        run_server(port);
    } else {
        block_size = (int) config.get("block_size").get_int64();
        if (getenv("BLKSIZE")) {
            block_size = (int) atoi(getenv("BLKSIZE"));
        }
        nr_threads = argc < 2 ? 1 : atoi(argv[1]);
        // connections = argc < 4 ? 1 : atoi(argv[3]);
        std::vector<std::string> server_list;
        JsonConfig servers = config.get("servers");
        for (int i = 0; i < servers.size(); ++i) {
            server_list.push_back(servers.get(i).get_str());
        }
        assert(!server_list.empty());
        run_client(server_list, port);
    }
    return 0;
}
