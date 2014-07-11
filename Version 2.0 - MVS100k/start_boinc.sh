#!/bin/bash

# Export BOINC library path variable
export LD_LIBRARY_PATH=/nethome/posypkin/SAT/lib:$LD_LIBRARY_PATH

# Read the input parameters
flagFile=$1
stopFile=$2
workDirectory=$3
boincDirectory=$4
((boincRunTime=$5))
((startTime = 0))
((currentTime = 0))
((sleepTime = 15))
((stopDelay = sleepTime*2))
((stopFileDetected = 0))
((lockFileOwner = 0))

startTime=$(date +%s)
currentTime=$startTime

echo "Parameters: flagFile = $flagFile; stopFile = $stopFile; boincDirectory = $boincDirectory; boincRunTime = $boincRunTime."

# Go to work directory and create a flag file
cd $workDirectory
lockfile -1 -r 1 $flagFile
((lockFileOwner = $?))

# Go to BOINC directory and start BOINC
if ((lockFileOwner == 0))
then
    cd $boincDirectory
    ./boinc&
fi
        
# Sleep cycle
cd $workDirectory
while ((currentTime - startTime <= boincRunTime && stopFileDetected == 0))
do
    sleep $sleepTime
    currentTime=$(date +%s)

    if [ -a $stopFile ]
    then
        ((stopFileDetected = 1))
        
        if ((lockFileOwner == 0))
        then
            sleep $stopDelay
            rm $stopFile
            echo "Detected the stop file $stopFile !"
        fi
    fi
done

# Update project, wait 30 second and correctly stop BOINC
if ((lockFileOwner == 0))
then
    cd $boincDirectory
    ./boinccmd --project http://sat.isa.ru/pdsat/ update
    echo "Exit code: " $? 
    sleep 30
    ./boinccmd --quit
fi

# Go to work directory and delete start flag file
if ((lockFileOwner == 0))
then
    cd $workDirectory
    rm -f $flagFile
fi
