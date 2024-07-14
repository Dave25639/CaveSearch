////David Tanase, CSCE 313-200, Spring 2024
#include "pch.h"
using namespace std;

//Define constants
#define CONNECT 0
#define DISCONNECT 1
#define MOVE 2

#define FAILURE 0
#define SUCCESS 1

#define STATUS_OK 0 // no error, command successful
#define STATUS_ALREADY_CONNECTED 1 // repeated attempt to connect
#define STATUS_INVALID_COMMAND 2 // command too short or invalid
#define STATUS_INVALID_ROOM 3 // room ID doesn't exist
#define STATUS_INVALID_BATCH_SIZE 4 // batch size too large or equals 0
#define STATUS_MUST_CONNECT 5

#define MAX_BATCH 5000

const DWORD BUF_SIZE = 128;

//Pack all structs
#pragma pack(push, 1)
class CommandCC {
public:
    UCHAR command : 2; // lower 2 bits
    UCHAR planet : 6; // remaining 6 bits
    DWORD cave; // which cave
    USHORT robots; // how many robots
};

class ResponseCC {
public:
    DWORD status;
    char msg[64];
};

class CommandRobotHeader {
public:
    DWORD command;
};

class ResponseRobotHeader {
public:
    DWORD status:3;
    DWORD len:29;
};
#pragma pack(pop)

class Pipe {
public:
    DWORD bytesAllocated;
    DWORD bytesReceived;
    char* readBuf;
    HANDLE pipe_handle;
    DWORD robotID;
    Pipe() { bytesAllocated = BUF_SIZE; readBuf = (char*)malloc(bytesAllocated); };
    ~Pipe() { free(readBuf); };

    Pipe(const Pipe&) = delete;
    Pipe& operator=(const Pipe&) = delete;

    void Connect(DWORD PID, int robotID) {
        //Create CommandRobot struct
        CommandRobotHeader initRobot;
        initRobot.command = CONNECT;

        //Create Robot pipename
        char pipename_Robot[1024];
        sprintf_s(pipename_Robot, "\\\\.\\pipe\\CC-%X-robot-%X", PID, robotID);

        //Wait for pipe to exist, then create file to read/write to Robot pipe
        while (WaitNamedPipe(pipename_Robot, INFINITE) == FALSE)
            Sleep(100); //TODO: Could optimize?
        pipe_handle = CreateFile(pipename_Robot, GENERIC_READ | GENERIC_WRITE, 0, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
        if (pipe_handle == INVALID_HANDLE_VALUE) {
            cout << "(-) Error creating robot pipe file: " << GetLastError() << endl;
            exit(-1);
        }

        //printf("Connecting to robot %d...\n", robotID);
        Send(&initRobot, sizeof(CommandRobotHeader));
        Read();
    }

    void Connect(DWORD PID, UCHAR planet, DWORD cave, USHORT numRobots) {
        //Create CommandCC struct
        CommandCC initCC;
        initCC.command = CONNECT;
        initCC.planet = planet;
        initCC.cave = cave;
        initCC.robots = numRobots;

        //Create CC pipename
        char pipename_CC[1024];
        sprintf_s(pipename_CC, "\\\\.\\pipe\\CC-%X", PID);

        //Wait for pipe to exist, then create file to read/write to CC pipe
        while (WaitNamedPipe(pipename_CC, INFINITE) == FALSE)
            Sleep(100); //TODO: could optimize?
        pipe_handle = CreateFile(pipename_CC, GENERIC_READ | GENERIC_WRITE, 0, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
        if (pipe_handle == INVALID_HANDLE_VALUE) {
            cout << "(-) Error creating CC pipe file: " << GetLastError() << endl;
            exit(-1);
        }

        printf("Connecting to CC with planet %d, cave %d, robots %d...\n", planet, cave, numRobots);
        Send(&initCC, sizeof(CommandCC));
        Read();
    }

    void Send(void* message, int size) {
        DWORD bytesWritten = 0;
        if (WriteFile(pipe_handle, message, size, &bytesWritten, NULL) == FALSE) {
            printf("(-) Error %d writing initialization instructions to CC.", GetLastError());
        }
    }

    void Read() {
        if (ReadFile(pipe_handle, readBuf, BUF_SIZE, &bytesReceived, NULL) == FALSE) {
            printf("(-) Error %d attempting to ReadFile with handle CC.", GetLastError());
            exit(-1);
        }
        
        if (bytesReceived == bytesAllocated) {
            DWORD remainder;
            if (PeekNamedPipe(pipe_handle, NULL, 0, NULL, &remainder, NULL) == FALSE) {
                printf("(-) Error %d attempting to PeekNamedPipe with handle CC.", GetLastError());
                exit(-1);
            }
            //cout << "(-) PeekNamedPipe Success: discovered " << remainder << " more bytes to be read." << endl;

            if (remainder > 0) {
                //Reallocate the array to be correct size
                char* temp = (char*) realloc(readBuf, bytesAllocated + remainder);
                readBuf = temp;
                DWORD additionalBytes;
                //Continue reading to buffer starting at the memory adress where we left off
                if (ReadFile(pipe_handle, readBuf + bytesAllocated, remainder, &additionalBytes, NULL) == FALSE) {
                    printf("(-) Error %d attempting to ReadFile with handle CC.", GetLastError());
                    exit(-1);
                }
                bytesReceived += additionalBytes;
                // cout << "(-) ReadFile Success: read " << br << " more bytes for a total of " << totalBytesRead << " bytes." << endl;
            }
        }
    }

    void printCC() {
        ResponseCC* r_CC = (ResponseCC*) readBuf;
        printf("CC says: status = %d, msg = '%s'\n", r_CC->status, r_CC->msg);
        if (r_CC->status == 0) {
            printf("Connection error, quitting...\n");
            exit(-1);
        }
    }

    void printRobot() {
        ResponseRobotHeader* r_Robot = (ResponseRobotHeader*) readBuf;
        //printf("Robot says: status = %d, msg = '%s'\n", r_Robot->status, r_Robot->msg);
        DWORD* initialRoom = (DWORD*)(r_Robot + 1);
        //printf("Current position: room %llX, light intensity %.2f\n", initialRoom->node, initialRoom->intensity);
        //if (r_Robot->status == 0) {
        //    printf("Connection error, quitting...\n");
        //}
    }

    void DisconnectCC() {
        CommandCC disconCC;
        disconCC.command = DISCONNECT;
        disconCC.planet = 0;
        disconCC.cave = 0;
        disconCC.robots = 0;

        Send(&disconCC, sizeof(CommandCC));
        if (CloseHandle(pipe_handle) == FALSE) {
            printf("(-) Error %d closing CC Handle.", GetLastError());
        }
    }

    void DisconnectRobot() {
        CommandRobotHeader disconRobot;
        disconRobot.command = DISCONNECT;

        Send(&disconRobot, sizeof(CommandRobotHeader));
        if (CloseHandle(pipe_handle) == FALSE) {
            printf("(-) Error %d closing Robot Handle.", GetLastError());
        }
    }

    DWORD* getInitRoom() {
        ResponseRobotHeader* r_Robot = (ResponseRobotHeader*)readBuf;
        DWORD* initialRoom = (DWORD*)(r_Robot + 1);
        return initialRoom;
    }
};

class UnexploredRoom {
public:
    DWORD ID;
    int distance;

    UnexploredRoom(DWORD i, int d) { ID = i; distance = d; };
    UnexploredRoom() { ID = 0; distance = 0; };
};

class Discovered {
public:
    set<DWORD> set;

    bool CheckAdd(DWORD roomID) {
        auto findElement = set.find(roomID);
        if (findElement == set.end()) {
            set.insert(roomID);
        }
    };

    void add(DWORD roomID) {
        set.insert(roomID);
    }

    bool find(DWORD roomID) {
        return set.find(roomID) != set.end();
    }
};

class Ubase {
public:
    virtual void push(DWORD roomID, int distance) = 0;
    virtual UnexploredRoom pop() = 0;
    virtual int pop(int batch, DWORD* batchBuf) = 0;
    virtual int size() = 0;
};

class U_BFS : public Ubase {
public:
    queue<UnexploredRoom> q;
    int sizeCount = 0;

    void push(DWORD roomID, int distance) {
        UnexploredRoom new_room(roomID, distance);
        sizeCount++;
        q.push(new_room);
    }

    UnexploredRoom pop() {
        UnexploredRoom next = q.front();
        q.pop();
        sizeCount--;
        return next;
    }

    int pop(int batch, DWORD* batchBuf) {
        int numPopped = 0;
        int s = size();
        for (int i = 0; i < min(batch, s); i++) {
            batchBuf[i] = pop().ID;
            numPopped++;
        }
        return numPopped;
    }

    int size() {
        return sizeCount;
    }
};

//Helper method to get current windows clock tick
LONGLONG getTime() {
    LARGE_INTEGER time;
    QueryPerformanceCounter(&time);
    return time.QuadPart;
}

//Create an instance of CC.exe
void createCCProcess(PROCESS_INFORMATION& pi, STARTUPINFO& s) {
    GetStartupInfo(&s);
    char path[] = "CC.exe";
    printf("Starting CC.exe...\n");
    if (CreateProcess(path, NULL, NULL, NULL, false, 0, NULL, NULL, &s, &pi) == FALSE)
    {
        printf("Error %d starting process %s.\n", GetLastError(), path);
        exit(-1);
    }
}

class MainThreadClass {
public:
    HANDLE sema;
    HANDLE mutex;
    HANDLE terminateEvent;
    HANDLE timerEvent;
    CPU cpu;

    Ubase* U;
    Discovered D;

    int activeThreads = 0;
    int threadsRunning = 0;
    int roomsExplored = 0; //E
    int roomsDiscovered = 0; //D
    int rExploredSinceLastPrintout = 0;
    int allNeighborsCount = 0;
    int uniqueRooms = 0;

    HANDLE eventListeners[2];
    HANDLE timerListeners[2];

    MainThreadClass(string searchType, UCHAR planet) {
        sema = CreateSemaphore(NULL, 0, 2147483647, NULL);
        mutex = CreateMutex(NULL, FALSE, NULL);
        terminateEvent = CreateEvent(NULL, TRUE, FALSE, NULL);
        timerEvent = CreateEvent(NULL, TRUE, TRUE, NULL);

        if (sema == NULL) {
            printf("CreateSemaphore error: %d\n", GetLastError());
            exit(-1);
        }

        if (mutex == NULL)
        {
            printf("CreateMutex error: %d\n", GetLastError());
            exit(-1);
        }

        if (terminateEvent == NULL)
        {
            printf("CreateEvent error: %d\n", GetLastError());
            exit(-1);
        }

        if (timerEvent == NULL)
        {
            printf("CreateEvent error: %d\n", GetLastError());
            exit(-1);
        }

        if (searchType == "BFS") {
            U = new U_BFS;
        }
        else {
            printf("(-) Error: Invalid searchType.\n");
            printf("(-) Valid inputs: [BFS]\n");
            exit(-1);
        }

        eventListeners[0] = terminateEvent;
        eventListeners[1] = sema;

        timerListeners[0] = terminateEvent;
        timerListeners[1] = timerEvent;
    };

    void RunSearch(DWORD cc_PID, int robotID);
    void TrackStats();
};

//Class shared with each thread
class ThreadParams {
public:
    DWORD cc_PID;
    int robotID;				// Thread sequence number between 0 and MAX_THREADS-1
    MainThreadClass* lpMTC;		// Pointer to a shared version of the class
};

void MainThreadClass::RunSearch(DWORD cc_PID, int robotID) { 
    Pipe pipe_Robot;
    pipe_Robot.Connect(cc_PID, robotID);
    pipe_Robot.printRobot();

    DWORD* initRoom = pipe_Robot.getInitRoom();

    bool terminate = false;

    char* request = new char[sizeof(CommandRobotHeader) + MAX_BATCH * sizeof(DWORD)];
    CommandRobotHeader* crh = (CommandRobotHeader*)request;
    crh->command = MOVE;
    DWORD* rooms = (DWORD*)(crh + 1);

    //Put first room in U and D
    DWORD mutexWaitResult = WaitForSingleObject(mutex, INFINITE);
    if (mutexWaitResult == WAIT_OBJECT_0) {
        threadsRunning++;
        if (D.find(*initRoom) == false) {
            U->push(*initRoom, 0);
            D.add(*initRoom);
            //printf("Pushed init room %llX\n", *initRoom);
            roomsExplored++;
            roomsDiscovered++;
            ReleaseSemaphore(sema, 1, NULL);
        }

        if (!ReleaseMutex(mutex)) {
            printf("Error with mutex release.\n");
            exit(-1);
        }
    }
    else {
        exit(-1);
    }

    while (true) {
        DWORD termEventResult = WaitForSingleObject(terminateEvent, 0);
        if (termEventResult == WAIT_OBJECT_0) {
            //printf("Terminate event signaled.");
            break;
        }

        int numRooms = 0;

        //Batch pop rooms
        mutexWaitResult = WaitForSingleObject(mutex, INFINITE);
        if (mutexWaitResult == WAIT_OBJECT_0) {
            numRooms = U->pop(MAX_BATCH, rooms);
            roomsExplored += numRooms;
            rExploredSinceLastPrintout += numRooms;

            if (numRooms != 0) {
                activeThreads++;
            }

            if (!ReleaseMutex(mutex)) {
                printf("Error with mutex release.\n");
                exit(-1);
            }
        }
        else {
            exit(-1);
        }

        if (numRooms == 0) {
            int randomSleepTime = rand() % 20 + 10;
            //Sleep(randomSleepTime);
            //Sleep(100);
            continue;
        }

        //Send move command
        pipe_Robot.Send(request, sizeof(CommandRobotHeader) + sizeof(DWORD) * numRooms);
        pipe_Robot.Read();

        int neighborsAdded = 0;
        int offset = 0;

        //Add neighbors to U and D
        mutexWaitResult = WaitForSingleObject(mutex, INFINITE);
        if (mutexWaitResult == WAIT_OBJECT_0) {
            while (offset + sizeof(ResponseRobotHeader) <= pipe_Robot.bytesReceived) {
                ResponseRobotHeader* curr_header = (ResponseRobotHeader*)(pipe_Robot.readBuf + offset);

                if (curr_header->status != STATUS_OK) {
                    printf("Error %d when receiving neighbors.", curr_header->status);
                    exit(-1);
                }

                else if (curr_header->len == 0) {
                    printf("Exit found.\n");
                    //TODO: extract which one was the exit from rooms buffer.
                }

                for (int i = 0; i < curr_header->len; i++) {
                    DWORD* currNeighbor = (DWORD*)(curr_header + 1) + i;

                    if (D.find(*currNeighbor) == false) {
                        U->push(*currNeighbor, 0); //update right depth
                        D.add(*currNeighbor);
                        uniqueRooms++;
                        roomsDiscovered++;
                        neighborsAdded++;
                    }
                }

                allNeighborsCount += curr_header->len;
                offset += sizeof(ResponseRobotHeader) + curr_header->len * sizeof(DWORD);
            }

            activeThreads--;
            if (U->size() == 0 && activeThreads == 0) {
                printf("Thread %d: reached empty queue!!\n", robotID);
                if (SetEvent(terminateEvent) == 0) {
                    printf("Error %d with set event.\n", GetLastError());
                    exit(-1);
                }
            }
            if (!ReleaseMutex(mutex)) {
                printf("Error %d with mutex release.\n", GetLastError());
                exit(-1);
            }
        }
        else {
            exit(-1);
        }

        if (neighborsAdded != 0) {
            if (ReleaseSemaphore(sema, neighborsAdded, NULL) == 0) {
                printf("Error %d with semaphore release.\n", GetLastError());
                exit(-1);
            }
        }
    }

    pipe_Robot.DisconnectRobot();
}

void MainThreadClass::TrackStats() {
    LARGE_INTEGER frequency;
    QueryPerformanceFrequency(&frequency);
    LONGLONG bootTime = getTime();
    LONGLONG startTime = getTime();
    LONGLONG currentTime;

    double delta = 2.0;
    double total_delta;

    double explored;
    double leftInQueue;
    double totalDiscovered;
    double rateOfRemovalFromQueue;
    double activeT;
    double percentUnique;

    double totalExploredAtLastPrint = 0;
    double totalDiscoveredAtLastPrint = 0;
    double totalUniqueAtLastPrint = 0;
    double totalAllNeighborsAtLastPrint = 0;

    while (true) {
        DWORD dwWaitResult = WaitForSingleObject(terminateEvent, 2000);
        if (dwWaitResult == WAIT_OBJECT_0) {
            break;
        }

        currentTime = getTime();
        total_delta = (double)((currentTime - bootTime) / frequency.QuadPart);
        explored = 1.0 * roomsExplored / 1000000.0;
        leftInQueue = 1.0 * U->size() / 1000000.0;
        totalDiscovered = 1.0 * roomsDiscovered / 1000000.0;
        rateOfRemovalFromQueue = (roomsExplored - totalExploredAtLastPrint) / delta;
        activeT = activeThreads;
        //double a = uniqueRooms - totalUniqueAtLastPrint;
        //double b = allNeighborsCount - totalAllNeighborsAtLastPrint;
        double a = uniqueRooms;
        double b = allNeighborsCount;
        percentUnique = a/b * 100;

        //printf("{%.0fs} [] E %.2fk, U %.2fk, D %.2fk, %d/sec, active %d, run %d\n", total_delta, rE, w, rD, rate, activeThreads, threadsRunning);
        printf("{%.0fs} [%0.1f M] U %.2f M D %.2f M, %0.0f rooms/sec, %d*, %.0f%% uniq [%.0f%% CPU, %.0f MB]\n", total_delta, explored, leftInQueue, totalDiscovered, rateOfRemovalFromQueue, activeT, percentUnique, cpu.GetCpuUtilization(NULL), cpu.GetProcessRAMUsage(true));
        totalExploredAtLastPrint = roomsExplored;
        totalDiscoveredAtLastPrint = roomsDiscovered;
        totalUniqueAtLastPrint = uniqueRooms;
        totalAllNeighborsAtLastPrint = allNeighborsCount;
        startTime = getTime();
    }
}

DWORD WINAPI InitializeThread(LPVOID p) {
    ThreadParams* t = (ThreadParams*) p;

    //SetThreadPriority(GetCurrentThread(), THREAD_PRIORITY_IDLE);
    if (t->robotID == -1) {
        t->lpMTC->TrackStats();
    }
    else {
        t->lpMTC->RunSearch(t->cc_PID, t->robotID);
    }

    return 0;
}

//Usage: CaveSearch.exe <numericValue_Planet> <numericValue_Cave> <numThreads> <searchType>
//Valid searchTypes: [BFS, DFS, bFS, A*] 
int main(int argc, char* argv[]) {
    //Start Timer
    LONGLONG initTime = getTime();

    //Check Sysargs
    if (argc != 5) {
        printf("(-) Usage: CaveSearch.exe <numericValue_Planet> <numericValue_Cave> <numThreads> <searchType>\n");
        return 1;
    }

    //Startup Procedures
    UCHAR planet = (UCHAR)atoi(argv[1]);
    DWORD cave = (DWORD)atoi(argv[2]);
    const int numThreads = atoi(argv[3]);
    string searchType = argv[4];

    PROCESS_INFORMATION pi_CC;
    STARTUPINFO s_CC;
    DWORD cc_PID;

    //Start CC.exe
    createCCProcess(pi_CC, s_CC);
    cc_PID = pi_CC.dwProcessId;

    Pipe pipe_CC;
    pipe_CC.Connect(cc_PID, planet, cave, numThreads);
    pipe_CC.printCC();

    //Initialize Threads
    HANDLE* threadHandles = new HANDLE[numThreads];
    ThreadParams* t = new ThreadParams[numThreads];
    MainThreadClass mtc(searchType, planet);

    printf("Starting to search with threads using %s...\n", searchType.c_str());

    printf("-------------------------\n");

    HANDLE statsHandle;
    ThreadParams statsParams;
    statsParams.robotID = -1;
    statsParams.lpMTC = &mtc;
    statsParams.cc_PID = cc_PID;

    if ((statsHandle = CreateThread(NULL, 0, InitializeThread, &statsParams, 0, NULL)) == NULL) {
        printf("(-) Error %d creating stats thread.", GetLastError());
        exit(-1);
    }

    for (int i = 0; i < numThreads; i++) {
        t[i].robotID = i;
        t[i].lpMTC = &mtc;
        t[i].cc_PID = cc_PID;

        if ((threadHandles[i] = CreateThread(NULL, 0, InitializeThread, &t[i], 0, NULL)) == NULL) {
            printf("(-) Error %d creating robot thread.", GetLastError());
            exit(-1);
        }
    }

    //Wait For Thread Termination
    for (int i = 0; i < numThreads; i++)
    {
        if (WaitForSingleObject(threadHandles[i], INFINITE) == WAIT_FAILED) {
            printf("(-) Error %d waiting for thread termination.", GetLastError());
        }
        if (CloseHandle(threadHandles[i]) == 0) {
            printf("(-) Error %d closing thread handle.", GetLastError());
        }
    }

    printf("-------------------------\n");

    //Termination Procedures
    pipe_CC.DisconnectCC();

    //Execution Time Output
    WaitForSingleObject(pi_CC.hProcess, INFINITE);

    LONGLONG finalTime = getTime();
    LARGE_INTEGER frequency;
    QueryPerformanceFrequency(&frequency);
    double time = (double) ((finalTime - initTime) / frequency.QuadPart);

    double rE = 1.0 * mtc.roomsExplored / 1000.0;
    double w = 1.0 * mtc.U->size() / 1000.0;
    double rD = 1.0 * mtc.roomsDiscovered / 1000.0;
    int rate = 1.0 * mtc.roomsExplored / time;
    printf("[final] E %.2fk, U %.2fk, D %.2fk, %d/sec, active %d, run %d\n", rE, w, rD, rate, 0, 0);

    printf("Execution time %.2f seconds\n", time);
}
