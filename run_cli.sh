ssh -tt wangyuansen@10.77.110.158 'cd Right/build && ./shard/shard_bench' &
ssh -tt wangyuansen@10.77.110.160 'cd Right/build && ./shard/shard_bench' &
# ssh -tt wangyuansen@10.77.110.159 'cd Right/build && ./shard/shard_bench' 
cd build && ./shard/shard_bench 