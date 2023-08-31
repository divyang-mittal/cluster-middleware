#!/bin/bash

#Job submit

function submit(){
	python3 -m cluster-middleware.master.user_scripts.submit.py -j $1
}

#Job kill

function job_kill(){
	python3 /home/ubuntu/cluster-middleware/master/user_scripts/kill.py -k $1
}

#Job stats

function queue_stat(){
	python3 /home/ubuntu/cluster-middleware/master/user_scripts/stat.py 
}
