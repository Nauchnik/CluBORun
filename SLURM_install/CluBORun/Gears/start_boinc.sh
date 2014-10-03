#!/bin/bash

# Export BOINC library path variable
export LD_LIBRARY_PATH=/home2/mpc1/SAT10P/lib:$LD_LIBRARY_PATH

# Read the input parameters
workDirectory=$1
nodeName=$2
startFlag=$3
stopFlag=$4
boincRunTime=$5
boincDirectory="${workDirectory}/${nodeName}"
((startTime = 0))
((currentTime = 0))
((sleepTime = 15))
((stopFlagDetected = 0))

startTime=$(date +%s)
currentTime=$startTime

echo "Parameters: workDirectory = $workDirectory; nodeName = $nodeName; boincRunTime = $boincRunTime."
echo "Allocated $SLURM_JOB_NUM_NODES nodes: $SLURM_JOB_NODELIST"

# Go to work directory and create a start-flag file
cd $workDirectory
touch $startFlag

# Go to BOINC directory and start BOINC
cd $boincDirectory
./boinc&

# Update project at startup
sleep $sleepTime
./boinccmd --project http://sat.isa.ru/pdsat/ update

# Sleep cycle
cd $workDirectory
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

# Update project, wait 30 second and correctly stop BOINC
cd $boincDirectory
./boinccmd --project http://sat.isa.ru/pdsat/ update

echo "Exit code: " $? 
sleep 30
./boinccmd --quit

# Go to work directory and delete start flag file
cd $workDirectory
rm -f $startFlag