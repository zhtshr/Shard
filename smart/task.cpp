
#include "smart/task.h"
#include "smart/common.h"

namespace sds {
    thread_local bool tl_task_pool_enabled = false;
    transfer_t TaskPool::g_empty_transfer;
}