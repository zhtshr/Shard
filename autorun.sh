#!/bin/bash
coros=(1 2 3 4)
threads=(1 2 4 8 12 16 18 24 32 38)
machines=(3)

for machine in ${machines[@]}; do
	for thread in ${threads[@]}; do
		for coro in ${coros[@]}  ; do
			echo "changing configs"
			./change_json.sh $thread $coro $machine
			sleep 2

			echo "./run_ser.sh"
			./run_ser.sh
			sleep 8

			echo "./run_cli.sh > result_${thread}_${coro}_${machine}.result"
			./run_cli.sh > result_${thread}_${coro}_${machine}.result
			sleep 2
			
			echo "./kill_ser.sh"
			./kill_all.sh
			sleep 3
		done done done
