# include <mpi.h>
# include <iostream>
# include <fstream>
# include <string>
# include <sstream>
# include <ctime>
# include <cstdio>
# include <stdlib.h>

using namespace std;

namespace CluBORun
{

class NodeInfo
{
public:
    NodeInfo(string name, string workDirectory, string machine);
    string Name;
    string WorkDirectory;
    string Machine;
};

NodeInfo::NodeInfo(string name, string workDirectory, string machine)
{
    Name = name;
    WorkDirectory = workDirectory;
    Machine = machine;
}


class TaskSettings
{
public:
    TaskSettings(string name, string workDirectory, string startFlag, string stopFlag, int duration);
    ~TaskSettings();
    string Name;
    string WorkDirectory;
    string StartFlag;
    string StopFlag;
    int Duration;
    int NodesCount;
    NodeInfo** Nodes;  
};

TaskSettings::TaskSettings(string name, string workDirectory, string startFlag, string stopFlag, int duration)
{
    Name = name;
    WorkDirectory = workDirectory;
    StartFlag = startFlag;
    StopFlag = stopFlag;
    Duration = duration;
    NodesCount = 0;
    Nodes = 0;
}

TaskSettings::~TaskSettings()
{
    if (Nodes != 0)
    {
	for (int i = 0; i < NodesCount; i++)
	{
	    if (Nodes[i] != 0)
	    {
		delete Nodes[i];
	    }
	}
    }
    delete[] Nodes;
}

class ProcessInfo
{
public:
    ProcessInfo(int processId, char* machine);
    int ProcessId;
    string Machine;
};

ProcessInfo::ProcessInfo(int processId, char* machine)
{
    ProcessId = processId;
    Machine = machine;
    int length = Machine.length();
    int ruPosition = 0;
    
    ruPosition = Machine.find("jscc.ru");
    
    if (ruPosition >= 0)
    {
	Machine = Machine.substr(0, ruPosition + 7);
    }
}

enum RunType
{
    Owner,
    Sleep
};

enum MessageTag
{
    HostName,
    Role,
    InstancePath,
    Shutdown
};

}

using namespace CluBORun;

int main(int argsCount, char** argsArray)
{
    // Переменные и константы ранжирования процессов
    const int coordinatorId = 0;	// Идентификатор процесса, который должен быть координатором
    int processId;			// Идентификатор данного процесса
    int processes;			// Общее число процессов
    
    // Переменные используемые для передачи сигналов через MPI
    int bufferSize;			// Размер буфера сообщения
    char* buffer;			// Указатель на буфер сообщения
    MPI_Status status;			// Состояние сигнала
    MPI_Request request;		// Объект (идентификатор?) MPI-сигнала об отправке названия машины
    MPI_Request roleRequest;		// Запрос на отправку роли процесса
    MPI_Request instanceRequest;	// Запрос на отправку пути к экземпляру BOINC
    MPI_Request shutdownRequest;	// Запрос на отправку сигнала остановки остановку
    int dummy;				// Вспомогательная переменная для приёма параллелью сигнала об остановке
    MPI_Request parallelShutdownRequest;// Запрос получения сигнала об остановке
    MPI_Status parallelShutdownStatus;	// Статус получения сигнала об остановке

    // Переменные организационной структуры MPI-задачи
    int hostNameLength = 0;		// Длина названия машины, на которой запущен MPI-процесс
    char* hostName = 0;			// Название физической машины, на которой запущен MPI-процесс
    ProcessInfo** answers = 0;		// Будущий массив указателей на сообщения параллелей о том, где они запущены
    int physicalNodesCount = 0;		// Число физических машин, на которых запущены MPI-процессы задачи
    int* boincOwners = 0;		// Будущий массив указателей на процессы, которые должны будут запускать BOINC на своих физических узлах
    string currentNode;			// Вспомогательная переменная - название текущего обрабатываемого узла
    
    TaskSettings* settings = 0;		// Набор настроек MPI-задачи
    string taskName;			// Название задачи
    string taskWorkDirectory;		// Рабочий каталог задачи
    string taskStartFlag;		// Start-флаг задачи
    string taskStopFlag;		// Stop-флаг задачи
    string taskRunTimeArg;		// Время работы задачи
    int taskNodesCount = 0;		// Число узлов, соответствующих задаче, вычисленное из числа параметров
    int taskRunTime = 0;		// Время работы задачи (уже в виде int)
    int parametersReaded = 0;		// Флаг успешного считывания параметров
    
    string startFlagPath;		// Путь к файлу-флагу запуска задачи, который выставляет координатор
    string stopFlagPath;		// Путь к файлу-флагу остановки задачи, на который реагирует координатор
    
    RunType processRole;		// Роль процесса в MPI-задаче
    RunType parallelRole;		// Тип запуска задаваемая координатором параллели
    string instancePath;		// Путь к экземпляру BOINC
    char* instancePathBuffer = 0;	// Буффер для считывания пути к экземпляру BOINC
    char* instanceWorkDirectory = 0;	// Массив для отправки рабочего каталога к экземпляру BOINC
    int instancePathBufferSize;		// Размер буфера для считывания пути к экземпляру BOINC
    MPI_Request instancePathRequest;	// Запрос на получение пути к экземпляру BOINC
    MPI_Status instancePathStatus;	// Состояние сигнала передачи пути к экземпляру BOINC
    string osCommand;			// Вспомогательная переменная для хранения текста команд OS


    // Инициализация MPI-задачи, получение идентификатора процесса и общего числа процессов в задаче    
    MPI_Init(&argsCount, &argsArray);
    MPI_Comm_rank(MPI_COMM_WORLD, &processId);
    MPI_Comm_size(MPI_COMM_WORLD, &processes);

    // Выполнение MPI-задачи
    if (processId == coordinatorId)
    {	
    
	// Выполнение MPI-задачи координатора
	
	    cout << "Main process start to receive messages..." << endl;

	    // Определение процессов, запускающих экземпляры BOINC
		// Формирование списка указателей на информацию о процессах
		answers = new ProcessInfo*[processes];
		answers[processId] = new ProcessInfo(processId, "BOINC Task Owner");
	
		// Обработка сообщений от процессов об их Id и названии машины
		for (int parallelId = 1; parallelId < processes; parallelId++)
		{
	    	    MPI_Probe(parallelId, (MessageTag)HostName, MPI_COMM_WORLD, &status);
	    	    MPI_Get_count(&status, MPI_CHAR, &bufferSize);
	    	    buffer = new char[bufferSize];
	    	    MPI_Recv(buffer, bufferSize, MPI_CHAR, parallelId, MPI_ANY_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

		    answers[parallelId] = new ProcessInfo(parallelId, buffer);
		    delete[] buffer;
	    
		    cout << "Message from parallel # " << answers[parallelId]->ProcessId << ". Host name is: " << answers[parallelId]->Machine << endl;
		}
	
		// Распределение процессов по физическим узлам
		currentNode.clear();
		physicalNodesCount = 0;
		boincOwners = new int[processes];	// Создание массива избыточной длинны, чтобы гарантированно в него уместиться
	
		for (int parallelId = 1; parallelId < processes; parallelId++)
		{
		    cout << "Parallel ID: " << parallelId << ", currentNode: " << currentNode << ", parallel machine: " << answers[parallelId]->Machine << endl;

		    if (answers[parallelId]->Machine != currentNode)
		    {
			cout << " Detected new owner - parallel id = " << parallelId << endl;
			
			boincOwners[physicalNodesCount] = parallelId;
			physicalNodesCount++;
			currentNode = answers[parallelId]->Machine;
		    }
		}
	
		cout << "MPI task run on " << physicalNodesCount << " nodes with following BOINC-owner processes: " << endl;
	
		for (int ownerId = 0; ownerId < physicalNodesCount; ownerId++)
		{
		    cout << "Process with ID = " << boincOwners[ownerId] << " run BOINC on node " << answers[boincOwners[ownerId]]->Machine << endl;
		}

	    // Считывание и проверка корректности входных параметров
	    if (argsCount >= 6)
	    {
		// Считывание заданных параметров
		    // Считывание атрибутов задачи
		    taskName = argsArray[1];
		    taskWorkDirectory = argsArray[2];
		    taskStartFlag = argsArray[3];
		    taskStopFlag = argsArray[4];
		    taskRunTimeArg = argsArray[5];
		    
		    istringstream runTimeConverter(taskRunTimeArg);
		    taskNodesCount = argsCount - 6;

		    if (!(runTimeConverter >> taskRunTime))
		    {
			taskRunTime = 0;
		    }
		    
		    for (int i = 0; i < argsCount; i++)
		    {
			cout << "Argument " << i << ": " << argsArray[i] << endl;
		    }

		    // Формирование объекта - настроек задачи
		    settings = new TaskSettings(taskName, taskWorkDirectory, taskStartFlag, taskStopFlag, taskRunTime);
		    
		    // Считывание и задание списка экземпляров BOINC, указанных в параметрах задачи, его сопоставление с физическими узлами
		    if (taskNodesCount >= physicalNodesCount)
		    {
			settings->NodesCount = physicalNodesCount;
			settings->Nodes = new NodeInfo*[physicalNodesCount];
		    
			for (int nodeId = 0; nodeId < physicalNodesCount; nodeId++)
	    		{
			    settings->Nodes[nodeId] = new NodeInfo(argsArray[nodeId + 6], settings->WorkDirectory + "/" + argsArray[nodeId + 6], answers[boincOwners[nodeId]]->Machine);
			    cout << "Node info: ID = " << nodeId << "; Name = " << settings->Nodes[nodeId]->Name << "; Work directory = " << settings->Nodes[nodeId]->WorkDirectory << "; Machine = " << settings->Nodes[nodeId]->Machine << endl;
			}

		    }
		    else
		    {
			cout << "Number of BOINC instances - " << taskNodesCount << " smaller than number of provided nodes - " << physicalNodesCount << endl;
		    }
		    
		    // Выставление флага успешного считывания параметров
		    parametersReaded = 1;
		    
	    }
	    else
	    {
		cout << "Invalid arguments! Minumun: 6, supplied: " << argsCount << endl;
		parametersReaded = 0;
	    }
	    
	    // Выполнение расчётов в рамках BOINC-инфраструктуры
	    if (parametersReaded == 1)
	    {
		// Выполнение расчётов в соответствии с заданными параметрами
		    // Задание путей к флагам запуска и остановки
		    string startFlagPath = settings->WorkDirectory + "/" + settings->StartFlag;
		    string stopFlagPath = settings->WorkDirectory + "/" + settings->StopFlag;

		    // Выставление флага о запуске экземпляра
	    	    osCommand = "touch " + startFlagPath;
	    	    cout << "Set a start flag by command '" << osCommand << "'" << endl;
	    	    system(osCommand.c_str());
		
		    int ownerToNotify = 0;	// Идентификатор оповещаемого владельца экземпляра BOINC
		    // Рассылка уведомлений параллелям с указанием их роли
		    for (int parallelId = 0; parallelId < processes; parallelId++)
		    {
			// Обработка очередного процесса
			if (parallelId != coordinatorId)
			{
			    // Отправка сообщения очередной параллели
			    if (parallelId != boincOwners[ownerToNotify])
			    {
				// Отправка сообщения параллели, которая должна только вести отсчёт времени
				parallelRole = (RunType)Sleep;
				cout << "Process with ID = " << parallelId << " get sleep role" << endl;
				MPI_Isend(&parallelRole, 1, MPI_INT, parallelId, (MessageTag)Role, MPI_COMM_WORLD, &roleRequest);
				MPI_Wait(&roleRequest, MPI_STATUS_IGNORE);
			    }
			    else
			    {
				// Отправка сообщения параллели, которая должна управлять экземпляром BOINC
				parallelRole = (RunType)Owner;
				cout << "Process with ID = " << parallelId << " get owner role" << endl;
				MPI_Isend(&parallelRole, 1, MPI_INT, parallelId, (MessageTag)Role, MPI_COMM_WORLD, &roleRequest);
				MPI_Wait(&roleRequest, MPI_STATUS_IGNORE);
				ownerToNotify++;
			    }
			    
			}
		    }
		    // Рассылка параллелям, запускающим BOINC информации о запускаемых экземплярах
		    for (int nodeId = 0; nodeId < settings->NodesCount; nodeId++)
		    {
			// Информирование параллели о том, какой экземпляр BOINC ей надо запускать
			cout << "Send BOINC instance info to parallel with ID = " << boincOwners[nodeId] << endl;
			instanceWorkDirectory = new char[settings->Nodes[nodeId]->WorkDirectory.length() + 1];
			settings->Nodes[nodeId]->WorkDirectory.copy(instanceWorkDirectory, settings->Nodes[nodeId]->WorkDirectory.length(), 0);
			instanceWorkDirectory[settings->Nodes[nodeId]->WorkDirectory.length()] = '\0';

			MPI_Isend(instanceWorkDirectory, settings->Nodes[nodeId]->WorkDirectory.length(), MPI_CHAR, boincOwners[nodeId], (MessageTag)InstancePath, MPI_COMM_WORLD, &instanceRequest);
			MPI_Wait(&instanceRequest, MPI_STATUS_IGNORE);
			
			delete[] instanceWorkDirectory;
			
		    }

		    // Отсчёт времени работы задачи
		    chdir(settings->WorkDirectory.c_str());

		    time_t startTime;
		    time_t currentTime = time(&startTime);
		    fstream stopFlag;
		    stopFlag.open(stopFlagPath.c_str(), std::fstream::in);

		    cout << "Start time: " << startTime << ". Current time: " << currentTime << endl;
		    cout << "Checking stop flag at " << stopFlagPath << endl;
		    
		    while (currentTime - startTime <= settings->Duration && !stopFlag.good())
		    {
			sleep(30);
			time(&currentTime);
			stopFlag.open(stopFlagPath.c_str(), std::fstream::in);
			cout << "Stop flag is " << stopFlag.good() << endl;
		    }
		    
		    cout << "After idle cycle stop flag is: " << stopFlag.good() << endl;
		    
		    if (stopFlag.good())
		    {
			cout << " Detected a stop flag: " << stopFlagPath << endl;
			remove(stopFlagPath.c_str());
		    }

		    // Рассылка параллелям уведомлений о необходимости завершения работы
		    for (int parallelId = 0; parallelId < processes; parallelId++)
		    {
			if (parallelId != coordinatorId)
			{
			    dummy = 1;
			    cout << "Shutdown parallel with ID = " << parallelId << endl;
			    MPI_Isend(&dummy, 1, MPI_INT, parallelId, (MessageTag)Shutdown, MPI_COMM_WORLD, &shutdownRequest);
			    MPI_Wait(&shutdownRequest, MPI_STATUS_IGNORE);
			}
		    }
		    
		    cout << "Coordinator stop the work." << endl;
		    
		    // Убирание флага о запуске экземпляра
		    cout << "Remove a start flag " << startFlagPath << endl;
		    remove(startFlagPath.c_str());
	    }
	    
	    // Высвобождение памяти
	    delete[] boincOwners;
	
	    for (int parallelId = 1; parallelId < processes; parallelId++)
	    {
		delete answers[parallelId];
    	    }
	    delete[] answers;
    }
    else
    {
	// Выполнение MPI-задачи в роли параллели
            // Определение названия машины, на которой запущен процесс
	    hostName = new char[MPI_MAX_PROCESSOR_NAME];
	    MPI_Get_processor_name(hostName,  &hostNameLength);

	    // Отправка сообщения с именем машины    
	    MPI_Isend(hostName, hostNameLength, MPI_CHAR, coordinatorId, (MessageTag)HostName, MPI_COMM_WORLD, &request);
	    MPI_Wait(&request, MPI_STATUS_IGNORE);

	    // Получение сообщения с указанием роли
	    MPI_Recv(&processRole, 1, MPI_INT, coordinatorId, (MessageTag)Role, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
	    cout << "Process " << processId << " get a role " << processRole << endl;
	    
	    // Запуск экземпляра BOINC
	    if (processRole == (RunType)Owner)
	    {
		// Запуск экземпляра BOINC процессом-координатором
		    // Получение информации о запускаемом экземпляре
	    	    MPI_Probe(coordinatorId, (MessageTag)InstancePath, MPI_COMM_WORLD, &instancePathStatus);
	    	    MPI_Get_count(&instancePathStatus, MPI_CHAR, &instancePathBufferSize);
	    	    instancePathBuffer = new char[instancePathBufferSize+1];
	    	    instancePathBuffer[instancePathBufferSize] = '\0';
	    	    MPI_Recv(instancePathBuffer, instancePathBufferSize, MPI_CHAR, coordinatorId, (MessageTag)InstancePath, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
	    	    instancePath = instancePathBuffer;	    	    
	    	    delete[] instancePathBuffer;
	    	    cout << "Parallel with ID = " << processId << " get a directive to start a BOINC instance at " << instancePath << endl;
	    	    cout << "Instance path buffer size: " << instancePathBufferSize << " " << endl;
	    	    
	    	    // Запуск экземпляра BOINC	    	
	    	    chdir(instancePath.c_str());
	    	    /*osCommand = "./boinc --redirectio &";*/ // Закомментировано из-за неясности размеров дисковой квоты
	    	    osCommand = "./boinc&";
	    	    cout << "Start BOINC by command: " << osCommand << endl;
        	    system(osCommand.c_str());
        	    
        	    // Пауза для запуска
        	    sleep(10);
        	    
        	    // Обновление состояния экземпляра
    		    osCommand = "./boinccmd --project http://sat.isa.ru/pdsat/ update";
		    cout << "Update project by command " << osCommand << endl;
		    system(osCommand.c_str());
	    }
	    else
	    {
		// Отчёт времени
		cout << "Parallel with ID = " << processId << " get a directive to sleep" << endl;
	    }

	    // Ожидание сигнала завершения работы
	    MPI_Irecv(&dummy, 1, MPI_INT, coordinatorId, (MessageTag)Shutdown, MPI_COMM_WORLD, &parallelShutdownRequest);
	    
	    int isRecieved = 0;
	    while (!isRecieved)
	    {
		sleep(30);
		MPI_Test(&parallelShutdownRequest, &isRecieved, &parallelShutdownStatus);
	    }
	    /*MPI_Wait(&parallelShutdownRequest, &parallelShutdownStatus);*/
	    cout << "Parallel with ID = " << processId << " goes to shutdown." << endl;
	    
	    if (processRole == (RunType)Owner)
	    {
		chdir(instancePath.c_str());
		osCommand = "./boinccmd --project http://sat.isa.ru/pdsat/ update";
		cout << "Update project by command " << osCommand << endl;
		system(osCommand.c_str());
		sleep(60);
		osCommand = "./boinccmd --quit";
		cout << "Shutdown BOINC by command " << osCommand << endl;
		system(osCommand.c_str());
	    }
	    
	    // Высвобождение памяти
	    delete[] hostName;
    }

    MPI_Finalize();

    return 0;
}
