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

#define MAX_BATCH 10000

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

class MyQueue {
    DWORD* buf;
    DWORD head, tail;
    DWORD bufSpaceAllocated;
    DWORD sizeQ;
public:
    MyQueue(int size) {
        buf = (DWORD*)calloc(size, sizeof(DWORD));
        head = 0;
        tail = 0;
        bufSpaceAllocated = size;
        sizeQ = 0;
    }
    ~MyQueue() {
        free(buf);
    }

    DWORD getSize() { return sizeQ; }
    
    void Push(DWORD item) {
        if (sizeQ == bufSpaceAllocated) {
            resize();
        }
        buf[tail] = item;
        tail = (tail + 1) % bufSpaceAllocated;
        sizeQ++;
    }

    void Push(DWORD* item_buf, DWORD num_items) {
        //for (int i = 0; i < num_items; i++) {
        //    Push(item_buf[i]);
        //}
        while (num_items > bufSpaceAllocated - tail) {
            resize();
        }

        memcpy(buf + tail, item_buf, num_items * sizeof(DWORD));

        sizeQ += num_items;
        tail = tail + num_items;
    }

    DWORD Pop(DWORD* array, int batchSize) {
        DWORD popCount = min(batchSize, sizeQ);
        DWORD spaceLeft = min(popCount, bufSpaceAllocated - head);

        memcpy(array, buf + head, spaceLeft * sizeof(DWORD));
        if (popCount > spaceLeft) {
            memcpy(array + spaceLeft, buf, (popCount - spaceLeft) * sizeof(DWORD));
        }

        head = (head + popCount) % bufSpaceAllocated;
        sizeQ -= popCount;

        return popCount;
    }

    void resize() {
        DWORD newSize = bufSpaceAllocated * 2;
        DWORD* newBuf = (DWORD*)calloc(newSize, sizeof(DWORD));

        if (head <= tail) {
            memcpy(newBuf, buf + head, sizeQ * sizeof(DWORD));
        }
        else {
            DWORD numTailItems = bufSpaceAllocated - head;
            memcpy(newBuf, buf + head, numTailItems * sizeof(DWORD));
            memcpy(newBuf + numTailItems, buf, (sizeQ - numTailItems) * sizeof(DWORD));
        }

        head = 0;
        tail = sizeQ;
        bufSpaceAllocated = newSize;

        free(buf);
        buf = newBuf;
    }
};

class MyHashMap {
    LONG* hashmap;
public:
    MyHashMap() {
        hashmap = (LONG*)calloc(pow(2, 27), sizeof(DWORD));
    }

    bool contains_insert(DWORD room) {
        int index = room >> 5;
        int bitPosition = room & 0x1F;

        return InterlockedBitTestAndSet(&hashmap[index], bitPosition) == 1;
    }
};

//Helper method to get current windows clock tick
LONGLONG getTime() {
    LARGE_INTEGER time;
    QueryPerformanceCounter(&time);
    return time.QuadPart;
}

string formatNumber(DWORD d) {
    string ret = "";
    int count = 0;

    while (d > 0) {
        if (count != 0 && count % 3 == 0) {
            ret += ',';
        }
        ret += (d % 10) + '0';
        d /= 10;
        count++;
    }

    string temp = "";

    for (int i = ret.size() - 1; i >= 0; i--) {
        temp += ret[i];
    }

    return temp;
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
    HANDLE terminateEvent;
    HANDLE timerEvent;
    CPU cpu;

    CRITICAL_SECTION cs;

    int curr_q = 0;
    int curr_level = 0;

    MyQueue* Q[2];
    MyHashMap H;

    int activeThreads = 0;
    int threadsRunning = 0;
    int roomsExplored = 0;
    double discovered = 0;
    int totalRooms = 0;

    double denominator = 0;

    MainThreadClass(string searchType, UCHAR planet) {
        InitializeCriticalSection(&cs);

        MyQueue* q1 = new MyQueue(100000);
        MyQueue* q2 = new MyQueue(100000);
        Q[0] = q1;
        Q[1] = q2;

        terminateEvent = CreateEvent(NULL, TRUE, FALSE, NULL);
        timerEvent = CreateEvent(NULL, TRUE, TRUE, NULL);

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
    HANDLE heap = HeapCreate(HEAP_NO_SERIALIZE, pow(2, 10), 0);
    if (heap == NULL) {
        printf("HeapCreate error: %d\n", GetLastError());
        exit(-1);
    }

    Pipe pipe_Robot;
    pipe_Robot.Connect(cc_PID, robotID);
    pipe_Robot.printRobot();
    DWORD* initRoom = pipe_Robot.getInitRoom();

    char* request = new char[sizeof(CommandRobotHeader) + MAX_BATCH * sizeof(DWORD)];
    CommandRobotHeader* crh = (CommandRobotHeader*)request;
    crh->command = MOVE;
    DWORD* rooms = (DWORD*)(crh + 1);

    DWORD* neighbors_buf = (DWORD*)HeapAlloc(heap, HEAP_NO_SERIALIZE, 100 * sizeof(DWORD));

    bool terminate = false;

    //Add first room
    EnterCriticalSection(&cs);
    threadsRunning++;
    if (H.contains_insert(*initRoom) == false) {
        Q[curr_q]->Push(*initRoom);

        roomsExplored++;
        discovered++;
    }
    LeaveCriticalSection(&cs);

    while (true) {
        DWORD termEventResult = WaitForSingleObject(terminateEvent, 0);
        if (termEventResult == WAIT_OBJECT_0) {
            break;
        }

        int numRooms = 0;

        //Batch pop rooms
        EnterCriticalSection(&cs);
        numRooms = Q[curr_q]->Pop(rooms, MAX_BATCH);
        roomsExplored += numRooms;
            
        if (numRooms > 0) {
            activeThreads++;
        }
        LeaveCriticalSection(&cs);

        if (numRooms == 0) {
            int randomSleepTime = rand() % 50 + 20;
            Sleep(randomSleepTime);
            continue;
        }

        //Send move command
        pipe_Robot.Send(request, sizeof(CommandRobotHeader) + sizeof(DWORD) * numRooms);
        pipe_Robot.Read();

        int curr_buf_capacity = (pipe_Robot.bytesReceived - sizeof(CommandRobotHeader) * numRooms) / sizeof(DWORD);
        DWORD* new_neighbors_buf = (DWORD*)HeapReAlloc(heap, HEAP_NO_SERIALIZE, neighbors_buf, curr_buf_capacity * sizeof(DWORD));

        if (new_neighbors_buf == NULL) {
            printf("HeapReAlloc error: %d\n", GetLastError());
            exit(-1);
        }

        neighbors_buf = new_neighbors_buf;

        int neighborsAdded = 0;
        int index = 0;
        int offset = 0;
        int totalNeighborsReceived = 0;

        while (offset + sizeof(ResponseRobotHeader) <= pipe_Robot.bytesReceived) {
            ResponseRobotHeader* curr_header = (ResponseRobotHeader*)(pipe_Robot.readBuf + offset);
            totalNeighborsReceived += curr_header->len;

            if (curr_header->status != STATUS_OK) {
                printf("Error %d when receiving neighbors.", curr_header->status);
                exit(-1);
            }

            else if (curr_header->len == 0) {
                printf("*** Thread [%d]: found exit %llX, distance %d, steps %d\n", robotID, rooms[index], curr_level, roomsExplored);
            }

            for (int i = 0; i < curr_header->len; i++) {
                DWORD* currNeighbor = (DWORD*)(curr_header + 1) + i;

                if (H.contains_insert(*currNeighbor) == false) {
                    neighbors_buf[neighborsAdded] = *currNeighbor;
                    neighborsAdded++;
                }
            }

            offset += sizeof(ResponseRobotHeader) + curr_header->len * sizeof(DWORD);
            index++;
        }

        EnterCriticalSection(&cs);
        //Add neighbors to Q
        denominator += totalNeighborsReceived;
        discovered += neighborsAdded;
        totalRooms += (pipe_Robot.bytesReceived - numRooms*sizeof(ResponseRobotHeader)) / sizeof(DWORD);
        Q[1-curr_q]->Push(neighbors_buf, neighborsAdded);

        activeThreads--;
        if (Q[curr_q]->getSize() == 0 && activeThreads == 0) {
            if (Q[1 - curr_q]->getSize() == 0) {
                printf("Thread %d: reached empty queue!!\n", robotID);
                if (SetEvent(terminateEvent) == 0) {
                    printf("Error %d with set event.\n", GetLastError());
                    exit(-1);
                }
            }
            else {
                curr_q ^= 1;
                curr_level++;
                printf("--------- Switching to level %d with %s nodes.\n", curr_level, formatNumber(Q[curr_q]->getSize()));
            }
        }
        LeaveCriticalSection(&cs);

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
    double leftInQueue;

    double totalExploredAtLastPrint = 0;

    while (true) {
        DWORD dwWaitResult = WaitForSingleObject(terminateEvent, 2000);
        if (dwWaitResult == WAIT_OBJECT_0) {
            break;
        }

        currentTime = getTime();
        total_delta = (double)((currentTime - bootTime) / frequency.QuadPart);
        leftInQueue = 1.0 * (Q[curr_q]->getSize() + Q[1-curr_q]->getSize());

        int percent = ((discovered / denominator) * 100);

        printf("{%.0fs} [%0.1fM] U %.2fM D %.2fM, %0.2fM/sec, %d*, %d%% uniq [%.0f%% CPU, %d MB]\n"
                ,total_delta
                ,((discovered - leftInQueue) / 1000000.0)
                ,(leftInQueue / 1000000.0)
                ,(discovered / 1000000.0)
                ,(((roomsExplored - totalExploredAtLastPrint) / delta) / 1000000.0)
                ,activeThreads
                ,percent
                ,cpu.GetCpuUtilization(NULL), cpu.GetProcessRAMUsage(true));

        totalExploredAtLastPrint = roomsExplored;
        startTime = getTime();
    }
}

DWORD WINAPI InitializeThread(LPVOID p) {
    ThreadParams* t = (ThreadParams*) p;

    //SetThreadPriority(GetCurrentThread(), THREAD_PRIORITY_IDLE);
    SetThreadAffinityMask(GetCurrentThread(), 0x15);
    
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

    //TODO: fix time, add average speed
    
    LARGE_INTEGER frequency;
    QueryPerformanceFrequency(&frequency);
    double total_delta = (double)(getTime() - initTime) / frequency.QuadPart;
    printf("Execution time %.2f seconds\n", total_delta);
    printf("Speed %0.2fM/sec\n", (mtc.discovered / total_delta) / 1000000.0);
}
