BEGIN {
    # Constants and variables for queue detection and planning nodes capture and freeing
    clusterNodes = 0;	  # This is a default value that should be override in "Load type detection" code block
    coresPerNode = 8;

    lineNumber = 0;
    totalCores = 0;
    blockedCores = 0;
    queueStartLine = 0;
    occupiedCores = 0;
    occupiedNodes = 0;
    applicationCores = 0;
    applicationNodes = 0;
    queuedCores = 0;
    freeCores = 0;
    boincFactCores = 0;
    boincPlanCores = 0;
    boincFactNodes = 0;
    boincPlanNodes = 0;
    queueLength = 0;

    freeCoresStart = 0;
    freeCoresFinish = 0;
    totalCoresStart = 0;
    totalCoresFinish = 0;
    blockedCoresStart = 0;
    blockedCoresFinish = 0;

    # Variables for load type detection
    loadType = "";
    day = "";
    hour = 0;
    dateCommand = "date";

    # Print the trace information
    system("date");
    print("Start the node catching and freeing");

    # Load type detection
    print("Load type detection started");
    dateCommand | getline;
    close(dateCommand);
    day = $1;
    hour = 0 + substr($4, 1, 2);
    
    if (day == "Sat" || day == "Sun" || hour < 8 || hour > 20)
    {
        loadType = "High";
	clusterNodes = 0;
    }
    else
    {
        loadType = "Low";
	clusterNodes = 0;
    }

    print("Day is: " day);
    print("Hour is: " hour);
    print("Now is " hour " hours of " day ". Choose Load type = " loadType " and set the clusterNodes = " clusterNodes);

    # Set the fields separator
    FS = ":";
    
    # Processing of tasks list
    while ("tasks" | getline > 0)
    {
        lineNumber++;

        # Number of total and blocked cores detection
        if (lineNumber == 2)
        {
	    freeCoresStart = index($0, "Free:") + 6;
	    freeCoresFinish = index($0, "of") - 1;
            totalCoresStart = index($0, "of") + 3;
            totalCoresFinish = index($0, "+");

	    if (freeCoresStart < freeCoresFinish)
	    {
		freeCores = substr($0, freeCoresStart, freeCoresFinish - freeCoresStart);
		print("Free: " freeCores);
	    }
	    
            if (totalCoresStart < totalCoresFinish)
            {
                totalCores = substr($0, totalCoresStart, totalCoresFinish - totalCoresStart);
                print("Total: " totalCores);
            }

            blockedCoresStart = index($0, "(") + 1;
	    blockedCoresFinish = index($0, "blocked") - 1;
	    
            if (blockedCoresStart < blockedCoresFinish)
	    {
                blockedCores = substr($0, blockedCoresStart, blockedCoresFinish - blockedCoresStart);
                print("Blocked: " blockedCores);
	    }
	}

        # Queue start detection
        if ($1 == "Queued")
        {
            queueStartLine = lineNumber;
        }

        # Processing of task record
        if (substr($0, 1, 3) != "===")
        {
	    # Adding cores to occupied by active tasks
	    if (lineNumber > 4 && queueStartLine == 0)
	    {
		occupiedCores += $3;

                if (index($10, "start_boinc.sh") > 0)
		{
		    boincFactCores += $3;
                }

                print("In process: " $3);
	    }

	    # Adding cores to queued cores and add task to queued tasks list
	    if (lineNumber > queueStartLine + 1 && queueStartLine > 0)
	    {
                queueLength++;
		queuedCores += $3;
		queuedTasks[queueLength] = 0 + $3;
		print("In queue: " $3);
	    }
	}
    }
    close("tasks");

    # Computing number of nodes occupied by tasks and BOINC
    applicationCores = occupiedCores - boincFactCores;
    applicationNodes = applicationCores/coresPerNode;
    occupiedNodes = occuipiedCores/coresPerNode;
    boincFactNodes = boincFactCores/coresPerNode;

    print("Total occupied cores: " occupiedCores);
    print("Total queued cores: " queuedCores);

    print("Tasks in queue: " queueLength);
    print("Cores in queued tasks:" queuedCores);
    
    boincPlanCores = clusterNodes*coresPerNode - applicationCores;
    for (i = 1; i <= queueLength; i++)
    {
	print("BOINC planned cores now: " boincPlanCores);
	print("Cores for queued task: " queuedTasks[i]);
	if (queuedTasks[i] <= boincPlanCores)
	{
	    boincPlanCores -= queuedTasks[i];
	    print("Task " i " can be started after freeing " queuedTasks[i] " cores");
	}
    }

    boincPlanNodes = boincPlanCores/coresPerNode;
    
    print("BOINC cores now: " boincFactCores);
    print("Planned BOINC cores: " boincPlanCores);

    FS = " ";    

    # Starting new BOINC hosts
    if (boincFactNodes < boincPlanNodes)
    {
        runTime = 43200;
        catchTime = runTime/60 + 10;
        started = 0;
        stopped = 0;
        startFlag = "";
        stopFlag = "";
        nodePath = "";
        startCommand = "";

        while ("ls -1 | grep .start" | getline > 0)
        {
	    startFlags[$1] = 1;
	}
	close("ls -1 | grep .start");

        while ((getline < "all_nodes.txt" > 0) && (boincFactNodes + started < boincPlanNodes))
        {
            if (startFlags[$1] + 0 == 0)
            {
                startFlag = $1;
                stopFlag = $2;
                nodePath = $3;
                print("Can start node ", $1, " at ", $3)
                started++;
                startCommand = "mpirun -np " coresPerNode " -p 6 -maxtime " catchTime " /home/zaikin/temp/BOINC/start_boinc.sh " startFlag " " stopFlag " " nodePath " " runTime;
                system(startCommand);
            }
        }
    }
    
    # Stop redundant BOINC hosts
    if (boincFactNodes > boincPlanNodes)
    {
	stopped = 0;
	stopFlag = "";
	touchCommand = "";

	while ("ls -1 | grep .start" | getline > 0)
	{
	    startFlags[$1] = 1;
	}
	
        while ((getline < "all_nodes.txt" > 0) && (boincFactNodes - stopped > boincPlanNodes))
        {
            if (startFlags[$1] + 0 == 1)
            {
                stopFlag = $2;
                stopped++;
                touchCommand = "touch " stopFlag;
                print("Set a shutdown flag ", stopFlag);
                print("Attached: ", boincFactNodes " Stopped: ", stopped, "Treshold: ", clusterNodes);
                system(touchCommand);
            }
        }
    }			

    # Print information into cluster_load.csv
    dateCommand = "date \"+%Y.%m.%d %H:%M\"";
    dateCommand | getline outline;
    close(dateCommand);
    print(outline ";" totalCores ";" blockedCores ";" occupiedCores ";" applicationCores ";" boincFactCores ";" freeCores ";" queuedCores) >> "cluster_load.csv";

    # Print trace information into stdout
    print("Node catching and freeing completed");
}