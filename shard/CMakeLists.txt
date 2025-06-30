add_executable(shard_backend backend.cpp shard.cpp)
target_compile_options(shard_backend PRIVATE -march=native)
target_link_libraries(shard_backend PUBLIC smart)

add_executable(shard_bench bench.cpp shard.cpp)
target_compile_options(shard_bench PRIVATE -march=native)
target_link_libraries(shard_bench PUBLIC smart)

add_executable(shard_bench_prof bench.cpp shard.cpp)
target_compile_definitions(shard_bench_prof PUBLIC -DCONFIG_STAT)
target_compile_options(shard_bench_prof PRIVATE -march=native)
target_link_libraries(shard_bench_prof PUBLIC smart)