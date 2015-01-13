#!/bin/bash

targetDirectory=""
currentDirectory=""
((tasksNumber = 0))
((instancesPerTask = 0))
((instancesNumber = 0))
((stringLength = 0))
lastChar=""

# Read the current directory
currentDirectory=$(pwd)

# Read input variables
echo "--------------------------------------------------------------------------------------"
echo "You run a CluBORun installer. Please input values, that need to CluBORun installation."
echo "--------------------------------------------------------------------------------------"
echo "Input existing path for CluBORun directory: "
read targetDirectory

echo "Input number of produced tasks: "
read tasksNumber

echo "Input number of produced BOINC instances per task: "
read instancesPerTask

# Delete last '/' symbol of present
stringLength=$(expr length $targetDirectory)
lastChar=$(expr substr $targetDirectory $stringLength 1)

if [ $lastChar == '/' ]
then
    ((stringLength = stringLength - 1))
    targetDirectory=$(expr substr $targetDirectory 1 $stringLength)
fi

echo $targetDirectory

# Copy CluBORun directory to specified path
cp -r CluBORun $targetDirectory

# Processing the catch_node.sh script
cd $targetDirectory/CluBORun/Gears
sed -e "s#%CLUBORUN_HOME%#${targetDirectory}/CluBORun#g" catch_node.txt > catch_node.sh
rm catch_node.txt
chmod u+x catch_node.sh


# Creating tasks and nodes
((instancesNumber = tasksNumber*instancesPerTask))
((startIndex = 1))
((finishIndex = instancesNumber))

instanceName=""
copyCommand=""

# Creating a matrix with links
cd $targetDirectory/CluBORun/Instances
mkdir matrix
cd matrix

for item in $(ls -1 ../BOINC)
do
    $(ln -s ../BOINC/$item $item)
done

# Copying matrix into required number of instances
cd $targetDirectory/CluBORun/Instances

for ((i = startIndex; i <= finishIndex; i++))
do
    instanceName="instance$(printf "%0*d\n" 3 $i)"
    copyCommand="cp -R matrix $instanceName"
    echo $instanceName "; " $copyCommand

    $($copyCommand)
done

# Remove matrix directory
rm -r matrix

# Generating tasks list - all_tasks.txt
((taskId = 0))
((instanceId = 0))
((instanceNumber = 0))
taskName=""
startFlag=""
stopFlag=""
taskLine=""

cd $targetDirectory/CluBORun/Gears

for ((taskId = 1; taskId <= tasksNumber; taskId++))
do
    taskName="task$(printf "%0*d\n" 3 $taskId)"
    startFlag="${taskName}.start"
    stopFlag="${taskName}.stop"
    taskLine=$taskName' '$startFlag' '$stopFlag

    for ((instanceId = 1; instanceId <= instancesPerTask; instanceId++))
    do
	((instanceNumber=(taskId - 1)*instancesPerTask + instanceId))
	instanceName="instance$(printf "%0*d\n" 3 $instanceNumber)"
	taskLine=$taskLine' '$instanceName
    done

    echo $taskLine >> all_tasks.txt
done