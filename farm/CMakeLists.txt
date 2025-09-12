add_executable(hopscotch_backend backend.cpp hopscotch.cpp)
target_compile_options(hopscotch_backend PRIVATE -march=native)
target_link_libraries(hopscotch_backend PUBLIC smart)

add_executable(hopscotch_bench bench.cpp hopscotch.cpp)
target_compile_options(hopscotch_bench PRIVATE -march=native)
target_link_libraries(hopscotch_bench PUBLIC smart)

add_executable(hopscotch_bench_prof bench.cpp hopscotch.cpp)
target_compile_definitions(hopscotch_bench_prof PUBLIC -DCONFIG_STAT)
target_compile_options(hopscotch_bench_prof PRIVATE -march=native)
target_link_libraries(hopscotch_bench_prof PUBLIC smart)