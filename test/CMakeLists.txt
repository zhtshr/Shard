add_executable(test_rdma test_rdma.cpp)
target_link_libraries(test_rdma PUBLIC smart)

add_executable(test_rdma_dyn test_rdma_dyn.cpp)
target_link_libraries(test_rdma_dyn PUBLIC smart)

add_executable(test_missing test_missing.cpp)
target_link_libraries(test_missing PUBLIC smart)