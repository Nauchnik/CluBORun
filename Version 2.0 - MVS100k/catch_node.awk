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
    coresPerNode = 8;
    nodes4Release = 0;
    cores4Release = 0;
    nodes4Start = 0;
    cores4Start = 0;

    # Variables for nodes start and release calculation
    startedNodes = 0;
    releasedNodes = 0;

    # Variables for nodes starting
    boincRunTime = 7200;
    catchNodeTime = boincRunTime/60 + 10;
    startFlag = "";
    stopFlag = "";
    workPath = "";
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
        if (substr($1, 1, 14) == "start_boinc.sh")
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

    print("Cores in queued task: " firstQueuedTaskCores ", Free cores: " freeCores ", BOINC cores: " boincCores);
    print("Nodes to release: " nodes4Release "; Nodes to start: " nodes4Start);

    # Starting the new BOINC nodes
    if (nodes4Start > 0 && boincTaskInQueue == 0)
    {
        # Read existing start flags
        while (getStartFlagsCommand | getline > 0)
        {
            startFlags[$1] = 1;
        }
        close(getStartFlagsCommand);

        # Read BOINC nodes info and start nodes
        while ((getline < "all_nodes.txt" > 0) && startedNodes < nodes4Start)
        {
            # Start node if it is not already running
            if (startFlags[$1] + 0 == 0)
            {
                startFlag = $1;
                stopFlag = $2;
                workPath = $3;
                nodePath = $4;
                print("Can start node " $1 " from " $4);
                startedNodes ++;
                startCommand = "mpirun -np " coresPerNode " -maxtime " catchNodeTime " /nethome/posypkin/SAT/start_boinc.sh " startFlag " " stopFlag " " workPath " " nodePath " " boincRunTime;
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
    if (nodes4Release > 0)
    {
        # Read existing start flags
        while (getStartFlagsCommand | getline > 0)
        {
            startFlags[$1] = 1;
        }
        close(getStartFlagsCommand);

        # Set the stop flag for running nodes
        while ((getline < "all_nodes.txt" > 0) && releasedNodes < nodes4Release)
        {
            # Check node for running
            if (startFlags[$1] + 0 == 1)
            {
                # Release node
                stopFlag = $2;
                releasedNodes ++;
                touchCommand = "touch " stopFlag;
                print("Set a shutdown flag " stopFlag);
                system(touchCommand);
            }
        }
    }
}