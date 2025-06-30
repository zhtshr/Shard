# Shard: A Scalable and Resize-optimized Hash Index on Disaggregated Memory
This project is built on top of smart (https://github.com/madsys-dev/smart).

## System Requirements
* **Mellanox InfiniBand RNIC** (ConnectX-6 in our testbed). Mellanox OpenFabrics Enterprise Distribution for Linux (MLNX_OFED) v5.3-1.0.0.1. Other OFED versions are under testing.

* **Build Toolchain**: GCC = 9.4.0 or newer, CMake = 3.16.3 or newer.

* **Other Software**: libnuma, clustershell, Python 3 (with matplotlib, numpy, python-tk)

## Build & Install

### Build `Shard`
Execute the following command in your terminal:
```bash
bash ./deps.sh
bash ./build.sh
```

## Evaluation

### Step 1: Setting Parameters

1. Set the server hostname, access granularity and type in `config/test_rdma.json`(for test_rdma):
   ```json
   {
      "servers": [
         "10.77.110.158"                  // the hostname of server side
      ],
      "port": 12345,                      // TCP port for connection establishation
      "block_size": 8,                    // access granularity in bytes
      "dump_file_path": "test_rdma.csv",  // path of dump file
      "type": "read"                      // can be `read`, `write` or `atomic`
   }
   ```
2. Set Shard cluster and workload in `config/datastructure_base.json`(for shard):
   ```json
   {
    "nr_threads": 1,
    "tasks_per_thread": 1,
    "dataset": "SepHash_uniform_RW",      // test with different workloads
    "dump_file_path": "datastructure.csv",
  
    "nic_numa_node": 1,
    "cpu_nodes": 2,
    "cores_per_cpu": 48,
  
    "insert_before_execution": true,
    "positive_search": false,
    "max_key": 100000000,
    "key_length": 8,
    "value_length": 8,
    "rehash_key": false,
    "operation_num": 100000000,
    "duration": 10,                        // run time(s)
    "intervals": 10,                       // number of intervals in the output result (uniformly divided)
    "display_interval_result": false,
    "zipfian_const": 0.99,
    "node_id" : 0,                         // in [0, 1, 2] for a three nodes cluster
    "client_num" : 3,
    "memory_servers": [
      {
        "hostname": "10.77.110.158",       // hostname of memory nodes
        "port": 12345
      },
      {
        "hostname": "10.77.110.159",
        "port": 12345
      },
      {
        "hostname": "10.77.110.160",
        "port": 12345
      }
    ]
   }
   ```
3. Set memory node parameters in `config/backend.json`(for shard):
   ```json
   {
    "dev_dax_path": "", 
    "capacity": 16000,  // amount of memory to use (MiB)
    "node_id": 0,       // in [0, 1, 2] for a three nodes cluster
    "tcp_port": 12345,  // Listen TCP port
    "nic_numa_node": 1  // Prefer bind socket
    "total_tasks": 1    // total tasks to predefine for sync
   }
   ```

### Step 2: Run Server
In server side, run the following command:
```bash
cd Shard/build
./shard/shard_backend
```

### Step 3: Run Client 
In another machine, run the following command:
```bash
cd Shard/build
./shard/shard_bench
```

## Contact
For any questions, please contact us at `zhahantian1206@ruc.edu.cn`.
