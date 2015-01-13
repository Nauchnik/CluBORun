#!/bin/bash

# Read the input parameters
((argumentsCount = $#))
taskName=$1
workPath=$2
startFlag=$3
stopFlag=$4
boincRunTime=$5
((instancesCount = argumentsCount - 5))
((startTime = 0))
((currentTime = 0))
((sleepTime = 15))
((stopFlagDetected = 0))
((argIndex = 0))
((nodeIndex = 0))
currentNode=""
((nodesCount = 0))
startCommand=""
stopCommand=""

startTime=$(date +%s)
currentTime=$startTime

echo "Parameters: taskName = $taskName; workPath = $workPath; boincRunTime = $boincRunTime."
echo "Allocated nodes: $nodesCount; $nodes"
echo "--------------------"
echo "Instances to run:"
# Create an array with BOINC instances
for ((instanceIndex = 0; instanceIndex < instancesCount; instanceIndex++))
do
    ((argIndex = instanceIndex + 5 + 1))
    instance[instanceIndex]=${!argIndex}
    echo ${instance[instanceIndex]}
done

echo "--------------------"
echo "Allocated nodes: $PBS_NODEFILE"

# Create an array with supplied nodes
for nodeName in $(cat $PBS_NODEFILE)
do
    if [[ $nodeIndex == "0" ]]
    then
        node[nodeIndex]=$nodeName
        echo ${node[nodeIndex]}
        currentNode=$nodeName
        echo "Recognizing: nodeIndex = $nodeIndex; node[nodeIndex] = ${node[nodeIndex]}; nodeName = $nodeName"
        ((nodeIndex = nodeIndex + 1))
    else
        if [[ $currentNode != $nodeName ]]
        then
            node[nodeIndex]=$nodeName
            echo "Recognizing: nodeIndex = $nodeIndex; node[nodeIndex] = ${node[nodeIndex]}; nodeName = $nodeName"
            currentNode=$nodeName
            ((nodeIndex = nodeIndex + 1))
        fi
    fi
done

((nodesCount = nodeIndex))

echo "--------------------"

# Go to work directory and create a start-flag file
sleep 5
cd $workPath
pwd
touch $startFlag

# Start BOINC instances on allocated nodes
for ((nodeIndex = 0; nodeIndex < nodesCount; nodeIndex++))
do
    (ssh -T ${node[nodeIndex]} << EOF
	cd ${workPath}/${instance[nodeIndex]}
	export LD_LIBRARY_PATH=${workPath}/BOINC/lib:$LD_LIBRARY_PATH
	nohup ./boinc > stdoutdae.txt &
	sleep 10
	./boinccmd --project http://sat.isa.ru/pdsat/ update
	exit
EOF
) &
done

# Sleep cycle
cd $workPath
while ((currentTime - startTime <= boincRunTime && stopFlagDetected == 0))
do
    sleep $sleepTime
    currentTime=$(date +%s)

    if [ -a $stopFlag ]
    then
        ((stopFlagDetected = 1))
        echo "Detected the stop flag file $stopFlag !"
        rm $stopFlag
    fi
done

# Stop BOINC instances on allocated nodes
for ((nodeIndex = 0; nodeIndex < nodesCount; nodeIndex++))
do
    (ssh -T ${node[nodeIndex]} << EOF
	cd ${workPath}/${instance[nodeIndex]}
	export LD_LIBRARY_PATH=${workPath}/BOINC/lib:$LD_LIBRARY_PATH
	./boinccmd --project http://sat.isa.ru/pdsat/ update
	sleep 30
	./boinccmd --quit
	exit
EOF
) &
done

sleep 30

# Go to work directory and delete start flag file
cd $workPath
rm -f $startFlag