
#ifndef SDS_YCSB_H
#define SDS_YCSB_H

#include "generator.h"

namespace sds {
    namespace util {
        enum OpType {
            INSERT = 0, READ, UPDATE, SCAN, REMOVE, READMODIFYWRITE
        };

        struct OpRecord {
            uint64_t type: 4;
            uint64_t scan_len: 8;
            uint64_t key: 52;
        };

        class WorkloadBuilder {
        public:
            WorkloadBuilder(size_t initial_elements, int *percentage, const std::string &key_dist, double zipfian_const, bool positive_search , uint64_t sep_range = 0 )
                    : positive_search_(positive_search), initial_elements_(initial_elements), counter_key(0), scan_len(1, 100) {
                for (int i = 0; i <= READMODIFYWRITE; ++i) {
                    op_type.addValue((OpType) i, percentage[i]);
                }
                counter_key.set(initial_elements);
                if (key_dist == "zipfian") {
                    int new_keys = (int) (initial_elements * percentage[INSERT] / 100 * 2); // a fudge factor
                    query_key = new ScrambledZipfianGenerator(initial_elements + new_keys, zipfian_const);
                    insert_key = new ScrambledZipfianGenerator(initial_elements + new_keys, zipfian_const);
                } else if (key_dist == "uniform") {
                    query_key = new UniformGenerator(0, initial_elements - 1);
                    insert_key = new UniformGenerator(0, initial_elements - 1);
                } else if (key_dist == "latest") {
                    query_key = new SkewedLatestGenerator(counter_key, zipfian_const);
                    insert_key = new SkewedLatestGenerator(counter_key, zipfian_const);
                } else if (key_dist == "counter") {
                    // insert_key.set(0);
                    query_key = new CounterGenerator(0);
                    insert_key = new CounterGenerator(0);
                } else {
                    assert(0);
                }
            }

            ~WorkloadBuilder() {
                if (query_key) {
                    delete query_key;
                    query_key = nullptr;
                }
            }

            void fill_record(OpRecord &record) {
                record.type = op_type.next();
                if (record.type == INSERT) {
                    record.key = insert_key->next();
                } else {
                    record.key = query_key->next();
                    if (positive_search_) {
                        record.key = record.key % initial_elements_ ;
                    }
                }
                if (record.type == SCAN) {
                    record.scan_len = scan_len.next();
                }
            }

            static WorkloadBuilder *Create(const std::string &name, size_t initial_elements, double zipfian_const, bool positive_search) {
                int percentage[6] = {0};
                std::string key_dist;
                if (name == "unitest") {
                    percentage[READ] = 10;
                    percentage[INSERT] = 90;
                    percentage[REMOVE] = 0;
                    key_dist = "zipfian";
                }
                else if (name == "ycsb-a") {
                    percentage[READ] = 50;
                    percentage[UPDATE] = 50;
                    key_dist = "zipfian";
                } else if (name == "ycsb-b") {
                    percentage[READ] = 95;
                    percentage[UPDATE] = 5;
                    key_dist = "zipfian";
                } else if (name == "ycsb-c") {
                    percentage[READ] = 100;
                    key_dist = "zipfian";
                } else if (name == "ycsb-d") {
                    percentage[READ] = 50;
                    percentage[INSERT] = 50;
                    key_dist = "uniform";
                } else if (name == "ycsb-e") {
                    percentage[SCAN] = 95;
                    percentage[INSERT] = 5;
                    key_dist = "zipfian";
                } else if (name == "ycsb-f") {
                    percentage[READ] = 50;
                    percentage[READMODIFYWRITE] = 50;
                    key_dist = "zipfian";
                } else if (name == "update-only") {
                    percentage[UPDATE] = 100;
                    key_dist = "zipfian";
                } else if (name == "read-only") {
                    percentage[READ] = 100;
                    key_dist = "zipfian";
                } else {
                    fprintf(stderr, "custom workload %s\n", name.c_str());
                    return nullptr;
                }
                return new WorkloadBuilder(initial_elements, percentage, key_dist, zipfian_const, positive_search);
            }

        private:
            bool positive_search_;
            size_t initial_elements_;
            DiscreteGenerator<OpType> op_type;
            CounterGenerator counter_key;
            UniformGenerator scan_len;
            Generator *query_key;
            Generator *insert_key;
        };
    }
}

#endif //SDS_YCSB_H
