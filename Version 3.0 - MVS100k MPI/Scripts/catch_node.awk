BEGIN {
    # Variables for task queue analysis
    lockedCores = 0;
    availableCores = 0;
    freeCores = 0;
    totalCores = 0;
    applicationCores = 0;
    boincCores = 0;
    firstQueuedTaskCores = 0;
    boincTaskInQueue = 0;

    # Variables for core and node balance calculation
    nodesPerTask = 8;
    coresPerNode = 8;
    tasks4Release = 0;
    nodes4Release = 0;
    cores4Release = 0;
    tasks4Start = 0;
    nodes4Start = 0;
    cores4Start = 0;

    # Variables for nodes start and release calculation
    startedNodes = 0;
    releasedNodes = 0;
    startedTasks = 0;
    releasedTasks = 0;

    # Variables for nodes starting
    boincRunTime = 7200;
    catchNodeTime = boincRunTime/60 + 10;
    taskName = "";
    startFlag = "";
    stopFlag = "";
    workPath = "";
    nodesList = "";
    nodesInTask = "";
    nodePath = "";
    startCommand = "";
    touchCommand = "";
    getStartFlagsCommand = "ls -1 | grep .start";

    # Variables for reporting and state quering
    currentDate = "";
    getClusterStateCommand = "mqinfo";
    getDateCommand = "date \"+%Y.%m.%d %H:%M\"";

    # Processing the tasks queue information
    while ((getClusterStateCommand | getline > 0) && totalCores == 0)
    {
        # Detection and processing of lines with running BOINC tasks
        if (substr($1, 1, 11) == "start_boinc")
        {
            if (firstQueuedTaskCores == 0)
            {
                boincCores += $6;
            }
            else
            {
        	boincTaskInQueue = 1;
            }
        }
        
	# Detection and processing of first queue lines
	if ($0 == "-- queue --")
	{
	    getClusterStateCommand | getline;
	    firstQueuedTaskCores = $6;
	}
	
	# Detection and processing of final task line with free/available statistics
        if ($1 == "Free:")
        {
            freeCores = $2;
            availableCores = $5;
            lockedCores = $7;
            applicationCores = availableCores - freeCores - boincCores;
            totalCores = lockedCores + availableCores;
        }
    }
    close(getClusterStateCommand);

    getDateCommand | getline currentDate;
    close(getDateCommand);
    print(currentDate ";" totalCores ";" lockedCores ";" availableCores ";" applicationCores ";" boincCores ";" freeCores ";" firstQueuedTaskCores) >> "cluster_load.csv";

    print("---- " currentDate " -----");

    # Calculation of the number of cores and nodes that should be released or occupied by BOINC
    if (firstQueuedTaskCores > freeCores && firstQueuedTaskCores - freeCores <= boincCores)
    {
        # Calculation of the number of cores that must be released
        cores4Release = firstQueuedTaskCores - freeCores;
        nodes4Release = int(cores4Release/coresPerNode);

        if (nodes4Release*coresPerNode < cores4Release)
        {
            nodes4Release ++;
        }
    }
    else
    {
        # Calculation of the number of cores and nodes that can be started
        cores4Start = freeCores;
        nodes4Start = int(cores4Start/coresPerNode);
    }

    # Calculation of the number of tashs than can be started of must be released
    tasks4Start = int(nodes4Start/nodesPerTask);
    tasks4Release = int(nodes4elease/nodesPerTask);
    
    if (tasks4Release*nodesPerTask < nodes4Release)
    {
	tasks4Release ++;
    }

    print("Cores in queued task: " firstQueuedTaskCores ", Free cores: " freeCores ", BOINC cores: " boincCores);
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
        while ((getline < "all_tasks.txt" > 0) && startedTasks < tasks4Start)
        {
            # Start node if it is not already running
            if (startFlags[$2] + 0 == 0)
            {
        	taskName = $1;
                startFlag = $2;
                stopFlag = $3;
                workPath = $4;
                
                nodesList = "";
                nodesInTask = NF - 4;
                for (fieldId = 5; fieldId <= NF; fieldId++)
                {
            	    print("Node " fieldId - 4 ": " $fieldId);
            	    nodesList = nodesList " " $fieldId;
                }
                
                startCommand = "mpirun VIADEV_USE_AFFINITY=0 -np " nodesInTask*coresPerNode " -maxtime " catchNodeTime " /home2/mpc1/SAT/start_boinc " taskName " " workPath " " startFlag " " stopFlag " " boincRunTime " " nodesList;
		print(startCommand);
                startedTasks ++;
                system(startCommand);
            }
        }
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
        while ((getline < "all_tasks.txt" > 0) && releasedTasks < tasks4Release)
        {
            # Check node for running
            if (startFlags[$2] + 0 == 1)
            {
                # Release node
                stopFlag = $3;
                releasedTasks ++;
                touchCommand = "touch " stopFlag;
                print("Set a shutdown flag " stopFlag);
                system(touchCommand);
            }
        }
    }
}