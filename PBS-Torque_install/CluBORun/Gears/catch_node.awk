BEGIN {
    # Variables for task queue analysis
    lockedNodes = 0;
    lockedCores = 0;
    availableNodes = 0;
    availableCores = 0;
    freeNodes = 0;
    freeCores = 0;
    totalNodes = 0;
    totalCores = 0;
    applicationNodes = 0;
    applicationCores = 0;
    boincNodes = 0;
    boincCores = 0;
    firstQueuedTaskNodes = 0;
    firstQueuedTaskCores = 0;
    boincTaskInQueue = 0;
    boincTaskName = ENVIRON["BOINCTaskName"];
    walkOnQueue = 0;

    # Variables for core and node balance calculation
    nodesPerTask = 1;
    coresPerNode = 32;
    tasks4Release = 0;
    nodes4Release = 0;
    tasks4Start = 0;
    nodes4Start = 0;
    taskHeadLength = 3;

    # Variables for nodes start and release calculation
    startedTasks = 0;
    releasedTasks = 0;

    # Variables for nodes starting
    boincRunTime = 3600;
    catchNodeTime = int(boincRunTime/3600) ":" int((boincRunTime - int(boincRunTime/3600)*3600)/60 + 10) ":00";
    taskName = "";
    startFlag = "";
    stopFlag = "";
    workPath = ENVIRON["InstancesDir"];
    mpiAppPath = ENVIRON["GearsDir"] "/start_boinc.sh";
    tasksListPath = ENVIRON["GearsDir"] "/all_tasks.txt";
    nodesList = "";
    nodesInTask = 0;
    nodePath = "";
    startCommand = "";
    touchCommand = "";
    getStartFlagsCommand = "ls " workPath " -1 | grep .start";

    # Variables for reporting and state quering
    currentDate = "";
    getFreeResourcesCommand = "qfree";
    getClusterStateCommand = "qstat -a";
    getDateCommand = "date \"+%Y.%m.%d %H:%M\"";
    clusterLoadCSV = ENVIRON["GearsDir"] "/cluster_load.csv";

    print("catchNodeTime = " catchNodeTime);
    print("workPath = " workPath);
    print("mpiAppPath = " mpiAppPath);
    print("clusterLoadCSV = " clusterLoadCSV);

    # Processing the tasks queue and free resources information
    # Reading information about overall cluster resources
    while (getFreeResourcesCommand | getline > 0)
    {
	# Detection and processing lines with cores and nodes info
	if ($1 == "Nodes")
	{
	    totalNodes = $3;
	    freeNodes = $5;
	    lockedNodes = $9;
	    applicationNodes = $7;
	}
	
	if ($1 == "Cores")
	{
	    totalCores = $3;
	    freeCores = $5;
	    lockedCores = $9;
	    applicationCores = $7;
	}
    }

    # Reading information about tasks in queue
    while (getClusterStateCommand | getline > 0)
    {
	# Processing a current line
	# Processing a line with job information
	if (walkOnQueue == 1)
	{
	    # Processing the job line
	    # Calculation resources attached to BOINC
	    if ($4 == boincTaskName)
	    {
		# Process a BOINC-task row
		# Processing the task that currently runs on cluster
		if ($10 == "R")
		{
		    boincNodes += $6;
		    boincCores += $7;
		}

		# Processing the task that currently falled into queue
		if ($10 == "Q")
		{
		    # Set the flag
		    boincTaskInQueue = 1;
		}
	    }

	    # Reading information about first queued task
	    if ($10 == "Q" && firstQueuedTaskCores == 0)
	    {
		firstQueuedTaskNodes = $6;
		firstQueuedTaskCores = $7;
	    }
	}

	# Processing a line with head table symbol
	if (substr($1, 1, 1) == "-")
        {
	    walkOnQueue = 1;
        }
    }
    close(getClusterStateCommand);

    # Calculate number of available nodes and cores and adjust number of application nodes and cores
    availableNodes = totalNodes - lockedNodes;
    availableCores = totalCores - lockedCores;
    applicationNodes = applicationNodes - boincNodes;
    applicationCores = applicationCores - boincCores;

    print("BOINC task name: " boincTaskName);
    print("Cluster queue state report:");
    print("Nodes: Total " totalNodes "; BOINC " boincNodes "; Free " freeNodes "; Locked " lockedNodes "; Available " availableNodes "; Application " applicationNodes ";")
    print("Cores: Total " totalCores "; BOINC " boincCores "; Free " freeCores "; Locked " lockedCores "; Available " availableCores "; Application " applicationCores ";")
    print("BOINC Task in queue: " boincTaskInQueue);
    print("First queued task: Nodes " firstQueuedTaskNodes "; Cores " firstQueuedTaskCores ";");


    # Write current cluster load to csv-file
    getDateCommand | getline currentDate;
    close(getDateCommand);
    print(currentDate ";" totalCores ";" lockedCores ";" availableCores ";" applicationCores ";" boincCores ";" freeCores ";" firstQueuedTaskCores) >> clusterLoadCSV;

    print("---- " currentDate " -----");

    # Calculation of the number of cores and nodes that should be released or occupied by BOINC
    if (firstQueuedTaskNodes > freeNodes && firstQueuedTaskNodes - freeNodes <= boincNodes)
    {
	# Calculation of the number of nodes that must be released
	nodes4Release = firstQueuedTaskNodes - freeNodes;
    }
    else
    {
	# Calculation of the number of nodes that can be started
	nodes4Start = freeNodes;
    }


    # Calculation of the number of tasks than can be started of must be released
    tasks4Start = int(nodes4Start/nodesPerTask);
    tasks4Release = int(nodes4Release/nodesPerTask);

    if (tasks4Release*nodesPerTask < nodes4Release)
    {
        tasks4Release ++;
    }

    print("Nodes in queued task: " firstQueuedTaskNodes ", Free nodes: " freeNodes ", BOINC nodes: " boincNodes);
    print("Nodes to release: " nodes4Release "; Tasks to release: " tasks4Release "; Nodes to start: " nodes4Start "; Tasks to start: " tasks4Start);

    # Starting the new BOINC nodes
    if (tasks4Start > 0 && boincTaskInQueue == 0)
    {
        # Read existing start flags
        while (getStartFlagsCommand | getline > 0)
        {
            startFlags[$1] = 1;
        }
        close(getStartFlagsCommand);

        # Read BOINC nodes info and start nodes
        while ((getline < tasksListPath > 0) && startedTasks < tasks4Start)
        {
            # Start node if it is not already running
            if (startFlags[$2] + 0 == 0)
            {
                taskName = $1;
                startFlag = $2;
                stopFlag = $3;

                nodesList = "";
                nodesInTask = NF - taskHeadLength;
                for (fieldId = taskHeadLength + 1; fieldId <= NF; fieldId++)
                {
                    print("Node " fieldId - taskHeadLength ": " $fieldId);
                    nodesList = nodesList " " $fieldId;
                }

                startCommand = "qsub -N Endurance -l nodes="nodesInTask ":ppn=32 -l walltime=" catchNodeTime " -F \"" taskName " " workPath " " startFlag " " stopFlag " " boincRunTime " " nodesList "\" " mpiAppPath;
                print(startCommand);
                startedTasks ++;
                system(startCommand);
            }
        }
        close(tasksListPath);
    }

    # Talk about BOINC tasks in queue
    if (boincTaskInQueue == 1)
    {
        print("New BOINC tasks fall into queue. Cannot start new tasks.");
    }

    # Release redundant BOINC nodes
    if (tasks4Release > 0)
    {
        # Read existing start flags
        while (getStartFlagsCommand | getline > 0)
        {
            startFlags[$1] = 1;
        }
        close(getStartFlagsCommand);

        # Set the stop flag for running nodes
        while ((getline < tasksListPath > 0) && releasedTasks < tasks4Release)
        {
            # Check node for running
            if (startFlags[$2] + 0 == 1)
            {
                # Release node
                stopFlag = $3;
                releasedTasks ++;
                touchCommand = "touch " workPath "/" stopFlag;
                print("Set a shutdown flag " stopFlag);
                system(touchCommand);
            }
        }
        close(tasksListPath);
    }
}