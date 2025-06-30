
#include "smart/thread.h"

namespace sds {
    thread_local ThreadCheckInCheckOut tl_tcico{};

    ThreadRegistry gThreadRegistry{};

    int gBindToCore[kMaxThreads] = {0};
    int gBindToSocket[kMaxThreads] = {0};

    void thread_registry_deregister_thread(const int tid) {
        gThreadRegistry.deregister_thread(tid);
    }

    StatInfo tl_stat[kMaxThreads];
}