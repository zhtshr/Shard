
#include "util/ycsb.h"

#include <thread>

using namespace sds::util;

int main(int argc, char **argv) {
    if (argc < 5) {
        fprintf(stderr, "Usage: %s <type> <initial_elements> <record_count> <dataset-path>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    int initial_elements = atoi(argv[2]);
    int record_count = atoi(argv[3]);

    WorkloadBuilder *builder = WorkloadBuilder::Create(argv[1], initial_elements, 0.99, 0);
    OpRecord *records = new OpRecord[record_count];
    if (!builder) {
        exit(EXIT_FAILURE);
    }

    FILE *fout = fopen(argv[4], "wb");
    if (!fout) {
        perror("fopen");
        exit(EXIT_FAILURE);
    }

    const static int kThreads = 8;
    std::thread workers[kThreads];
    for (int i = 0; i < kThreads; ++i) {
        workers[i] = std::thread([&](int tid) {
            int start_off = record_count / kThreads * tid;
            int stop_off = std::min(start_off + record_count / kThreads, record_count);
            for (int off = start_off; off < stop_off; ++off) {
                builder->fill_record(records[off]);
            }
        }, i);
    }

    for (int i = 0; i < kThreads; ++i) {
        workers[i].join();
    }

    if (fwrite(records, sizeof(OpRecord), record_count, fout) != record_count) {
        perror("fwrite");
    }

    fclose(fout);
    delete builder;
    return 0;
}
