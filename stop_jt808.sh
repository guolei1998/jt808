#!/bin/bash
PID=`ps -ef | grep -E "jt808server.py" | grep -v grep | awk '{print $2} ' | head -1`
if [ -z $PID ];then
	echo "process not found"
else
	ps -ef | grep -E "jt808server.py" | grep -v grep | awk '{print $2} ' | xargs kill -9
fi

