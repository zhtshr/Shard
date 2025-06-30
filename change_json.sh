if [ $# -lt 3 ] ;then
    echo "Your args: $0 $@"
    echo "Usage    : $0 thr_num coro_num node_num"
    exit
fi

machine=${3}
thread=${1}
coro=${2} 

tmp_json=$(cat ./config/datastructure_base.json | jq ".nr_threads = ${thread} | .tasks_per_thread = ${coro} | .client_num = ${machine} " )
echo "The json file is:"
echo $tmp_json

tmp_json=$(echo $tmp_json | jq ".node_id = 2")
# ssh -tt wangyuansen@10.77.110.227 "cd ~/Right && echo \"${tmp_json//\"/\\\"}\"  > ./config/datastructure.json"
ssh -tt wangyuansen@10.77.110.158 "cd ~/Right && echo \"${tmp_json//\"/\\\"}\" > ./config/datastructure.json"

tmp_json=$(echo $tmp_json | jq ".node_id = 0")
ssh -tt wangyuansen@10.77.110.159 "cd ~/Right && echo \"${tmp_json//\"/\\\"}\" > ./config/datastructure.json"

tmp_json=$(echo $tmp_json | jq ".node_id = 1")
ssh -tt wangyuansen@10.77.110.160 "cd ~/Right && echo \"${tmp_json//\"/\\\"}\" > ./config/datastructure.json"
