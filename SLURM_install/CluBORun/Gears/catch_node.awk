BEGIN {
    # Variables for task queue analysis
    boincTaskInQueue = 0;

    # Variables for nodes start and release calculation
    startedNodes = 0;
    releasedNodes = 0;

    # Variables for nodes starting
    boincRunTime = 14400;
    catchNodeTime = boincRunTime/60 + 10;
    startFlag = "";
    stopFlag = "";
    instanceName = "";
    workPath = ENVIRON["InstancesDir"];
    appPath = ENVIRON["GearsDir"] "/start_boinc.sh";
    tasksListPath = ENVIRON["GearsDir"] "/all_tasks.txt";
    startCommand = "";
    touchCommand = "";
    getStartFlagsCommand = "ls " workPath " -1 | grep .start";

    # Variables for reporting and state quering
    currentDate = "";
    getClusterStateCommand = "squeue";
    getDateCommand = "date \"+%Y.%m.%d %H:%M\"";

    # Processing the tasks queue information
    while ((getClusterStateCommand | getline > 0) && totalCores == 0)
    {
        # Detection and processing of lines with running BOINC tasks
        if (substr($3, 1, 8) == "start_bo")
        {
            if ($8 == "(Resources)" || $8 == "(Priority)" || $8 == "(ReqNodeNotAvail)")
            {
                boincTaskInQueue = 1;
            }
        }

    }
    close(getClusterStateCommand);

    getDateCommand | getline currentDate;
    close(getDateCommand);
    print("---- " currentDate " -----");

    nodes4Start = 1;

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
        while ((getline < tasksListPath > 0) && startedNodes < nodes4Start)
        {
            # Start node if it is not already running
            if (startFlags[$2] + 0 == 0)
            {
                startFlag = $2;
                stopFlag = $3;
                instanceName = $4;
                print("Can start node " instanceName);
                startedNodes ++;
                startCommand = "sbatch --nodes=1 --time=" catchNodeTime " --output " instanceName ".log " appPath " " workPath " " instanceName " " startFlag " " stopFlag " " boincRunTime;
                print(startCommand);
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

    # Set count of released nodes to 1 if exists task in queue (to provide "fast freeing" nodes from BOINC-tasks)
    if (boincTaskInQueue == 1)
    {
        nodes4Release = 1;
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
        while ((getline < tasksListPath > 0) && releasedNodes < nodes4Release)
        {
            # Check node for running
            if (startFlags[$2] + 0 == 1)
            {
                # Release node
                stopFlag = $3;
                releasedNodes ++;
                touchCommand = "touch " workPath "/" stopFlag;
                print("Set a shutdown flag " stopFlag);
                system(touchCommand);
            }
        }
        close(tasksListPath);
    }
}