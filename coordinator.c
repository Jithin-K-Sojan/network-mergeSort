#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <strings.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <signal.h>
#include <sys/wait.h>
#include <errno.h>

#define PORT 8080
#define MAX 50

int clSock = -1;
int servSock = -1;
int listenSock = -1;

void sigIntHandler(int signo){

    if(clSock!=-1){
        if(close(clSock)<0){
            printf("PID %d: clSock\n",getpid());
        }
    }
    
    if(servSock!=-1){
        if(close(servSock)<0){
            printf("PID %d: servSock\n",getpid());
        }
    }


    if(listenSock!=-1){
        if(close(listenSock)<0){
            printf("PID %d: listensSock\n",getpid());
        }
    }

    printf("PID %d: terminated!\n",getpid());
    exit(0);
}

int dummy(int a){
    return a;
}

int numOfChildren(int numEle){
    numEle-=1;
    int curr = 1;
    int numChild = 0;
    while(numEle!=0){
        numEle-=curr;
        numChild++;
        curr*=2;
    }

    return numChild;
}

int** getReturnArray(int numChild){

    int** retArr = (int**)malloc(sizeof(int*)*numChild);

    int curr = 1;
    for(int i = 0;i<numChild;i++){
        retArr[i] = (int*)malloc(sizeof(int)*curr);
        curr*=2;
    }

    return retArr;
}

int numOfElements(int nodeNum,int totalNum){
    int val = totalNum;

    while(val!=0){
        if(nodeNum%val==0){
            return val;
        }
        val/=2;
    }
    return -1;
}

void writeIntoResArray(int* arr,int** returnArray,int numChild,int isFileIp){

    int curr = 1;
    int index = 0;

    for(int i = 0;i<numChild;i++){
        for(int j = 0;j<curr;j++){
            if(isFileIp){
                returnArray[i][j] = arr[index];
            }
            else{
                returnArray[i][j] = ntohl(arr[index]);
            }
            index++;
        }
        curr *= 2;
    }
}

char* ipArray[MAX];
int portArray[MAX];
int nodeId;

void sendElements(int clSock,int** returnArray,int nodeNum,int numChild){

    int curr = 1;
    int n = 0;
    int src = nodeNum;
    struct sockaddr_in mySockName;
    int plen = sizeof(mySockName);
    getsockname(clSock,(struct sockaddr*)&mySockName,&plen);
    printf("IP: %s,PORT:%d\n",inet_ntoa(mySockName.sin_addr),ntohs(mySockName.sin_port));
    for(int i = 0;i<numChild;i++){
        int dest = nodeNum+curr;
        dest = dest;

        // Writing the destination host.
        dest = htonl(dest);
        if((n=write(clSock,&dest,sizeof(int)))<0){
            perror("Error writing to server socket!\n");
            exit(0);
        }
        dest = ntohl(dest);
        
        src = htonl(src);
        if((n=write(clSock,&src,sizeof(int)))<0){
            perror("Error writing to server socket!\n");
            exit(0);
        }
        src = ntohl(src);


        if((n=write(clSock,&mySockName,sizeof(mySockName)))<0){
            perror("Error writing to server socket!\n");
            exit(0);
        }

        printf("Node %d(relative %d): Sending unsorted numbers to IP: %s PORT: %d\n",nodeId,nodeNum,ipArray[dest],portArray[dest]);

        // Writing the elements to be sent to destination host.

        for(int k = 0;k<curr;k++){
            returnArray[i][k] = htonl(returnArray[i][k]);
        }

        if((n=write(clSock,returnArray[i],curr*sizeof(int)))<0){
            perror("Error writing to server socket!\n");
            exit(0);
        }

        for(int k = 0;k<curr;k++){
            returnArray[i][k] = ntohl(returnArray[i][k]);
        }

        curr = curr*2;
    }

}

void writeResIntoRetArr(int** returnArray,int src,int* buffer,int totalNum){
    int numEle = numOfElements(src,totalNum);
    int index = 0;
    int curr = 1;

    while(curr!=numEle){
        curr = curr*2;
        index++;
    }

    for(int i = 0;i<curr;i++){
        returnArray[index][i] = ntohl(buffer[i]);
    }
}

void convHtoNArr(int* buffer,int numEle){
    for(int i = 0;i<numEle;i++){
        buffer[i] = buffer[i];
    }
}

void merge(int valWithNode,int** returnArray,int* buffer,int numChild){

    int curr = 1;

    int prev[MAX];
    prev[0] = valWithNode;

    curr = 1;

    for(int i = 0;i<numChild;i++){

        int ind1 = 0;
        int ind2 = 0;
        int index = 0;
        while(ind1<curr && ind2<curr){
            if(returnArray[i][ind1]<=prev[ind2]){
                buffer[index] = returnArray[i][ind1];
                ind1++;
            }
            else{
                buffer[index] = prev[ind2];
                ind2++;
            }
            index++;
        }

        if(ind2==curr){
            while(ind1<curr){
                buffer[index] = returnArray[i][ind1];
                ind1++;
                index++;
            }
        }

        if(ind1==curr){
            while(ind2<curr){
                buffer[index] = prev[ind2];
                ind2++;
                index++;
            }
        }

        curr = curr*2;
        for(int j = 0;j<curr;j++){
            prev[j] = buffer[j];
        }
    }

}

int nodeMain(int nodeNum,int totalNum,int* arr,int listenSock1,int nodeId1,char* ipArray[],int portArray[]){

    listenSock = listenSock1;
    nodeId = nodeId1;

    signal(SIGINT,sigIntHandler);

    if(totalNum==1)return 1;

    clSock = socket(AF_INET,SOCK_STREAM,0);

    fd_set readSet,writeSet;

    struct sockaddr_in servAddr,cliAddr;
    int len = sizeof(struct sockaddr_in);
    bzero(&servAddr,sizeof(servAddr));
    servAddr.sin_family = AF_INET;
    int next = (nodeNum+1)%totalNum;
    servAddr.sin_addr.s_addr = inet_addr(ipArray[next]);
    servAddr.sin_port = htons(portArray[next]);

    int connectToClient = 0;
    int connectToServer = 0;

    int n = 0;

    if((n=connect(clSock,(struct sockaddr*)&servAddr,sizeof(struct sockaddr_in)))<0){
        perror("connect");
        exit(0);

    }
    else{
        connectToServer = 1;
    }

    servSock = -1;
    int maxfd = 0;

    fd_set aReadSet,aWriteSet;

    servSock = accept(listenSock,NULL,NULL);


    struct sockaddr_in clSockAddr;
    int lenAddr = sizeof(clSockAddr);

    int numEle = totalNum;
    int numChild = 0;
    int** returnArray = NULL;

    int numEleRecv = 0;
    
    if(numEle==1){
        numChild = 0;
    }
    else{
        numChild = numOfChildren(numEle);
        returnArray = getReturnArray(numChild);
    }

    int valWithNode = 0;
    int buffer[MAX];

    FD_ZERO(&readSet);
    FD_ZERO(&writeSet);

    FILE* opFile = NULL;

    writeIntoResArray(arr+1,returnArray,numChild,1);
    valWithNode = arr[0];
    numEleRecv+=1;

    sendElements(clSock,returnArray,nodeNum,numChild);
    maxfd = 0;

    FD_SET(clSock,&readSet);
    maxfd = maxfd>clSock?maxfd:clSock;


    while(1){

        aReadSet = readSet;
        n = select(maxfd+1,&aReadSet,NULL,NULL,NULL);
        if(n<0){
            perror("select error");
            exit(0);
        }

        if(FD_ISSET(clSock,&aReadSet)){
            int dest;
            if((n = read(clSock,&dest,sizeof(int)))<0){
                perror("read error3");
                exit(0);
            }
            dest = ntohl(dest);

            if(n==0){
                // Connection closed.
                continue;
            }

            int src;
            if(read(clSock,&src,sizeof(int))<0){
                perror("read error3");
                exit(0);
            }
            src = ntohl(src);

            if(read(clSock,buffer,sizeof(int)*numOfElements(dummy(src),totalNum))<0){
                perror("read error4");
                exit(0);
            }


            if(dummy(dest)==nodeNum){
                printf("Node %d(relative %d): received sorted numbers from IP: %s PORT: %d\n",nodeId,nodeNum,ipArray[src],portArray[src]);
                writeResIntoRetArr(returnArray,dummy(src),buffer,totalNum);
                numEleRecv+= numOfElements(dummy(src),totalNum);
            }
            else{
                dest = htonl(dest);
                if(write(servSock,&dest,sizeof(int))<0){
                    perror("write error");
                    exit(0);
                }
                dest = ntohl(dest);

                src = htonl(src);
                if(write(servSock,&src,sizeof(int))<0){
                    perror("write error");
                    exit(0);
                }
                src = ntohl(src);

                if(write(servSock,buffer,sizeof(int)*numOfElements(dummy(src),totalNum))<0){
                    perror("write error");
                    exit(0);
                }    
            }

        }

        if(numEleRecv==numEle){

            if(numEle==1){
                buffer[0] = valWithNode;
            }
            else{
                merge(valWithNode,returnArray,buffer,numChild);
            }

            for(int i = 0;i<numEle;i++){
                arr[i] = buffer[i];
            }
            close(clSock);
            return 1;

            numEleRecv = 0;

        }

    }

}

void killAllChildren(int childPids[],int numOfChild){
    for(int i = numOfChild;i>=0;i--){
        kill(childPids[i],SIGINT);
    }

    int waitVal,status;
    while(waitVal = wait(&status)>0);
}

int sfd = -1;
int sfd2 = -1;

void getIpPort(int N, char** ipArray, int* portArray){

    for(int i = 0;i<N;i++){
        ipArray[i] = (char*)malloc(sizeof(char)*16);
        memset(ipArray[i],0,16);
        ipArray[i] = strcpy(ipArray[i],"127.0.0.1");

        portArray[i] = PORT+i;
    }
}

FILE* getOpFile(char* filename){
    FILE* opFile = fopen(filename,"w");

    if(opFile==NULL){
        printf("Error opening Output File.\n");
        exit(0);
    }

    return opFile;
}


int main(int argc, char* argv[]){
    
    if(argc!=4){
        printf("Input should be ./a.out <number-of-elements> <root-node-id> <input-file>\n");
        exit(0);
    }

    int N = atoi(argv[1]);
    if(N==0){
        printf("No elements to sort!!");
        exit(0);
    }

    int nodeId = atoi(argv[2]);

    int testN = N;
    while(testN!=1){
        if(testN%2==1){
            printf("<number-of-elements> should be a power of 2.\n");
            exit(0);
        }
        testN/=2;
    }

    if(nodeId<0 || nodeId>N-1){
        printf("<root-node-id> should be between the range 0 and N-1.\n");
        exit(0);

    }

    getIpPort(N,ipArray,portArray);

    char* cmdArgs[MAX*2+4];
    int currInd = 4;
    for(int i = 0;i<N;i++){
        cmdArgs[currInd] = ipArray[i];
        char* portStr = (char*)malloc(sizeof(char)*7);
        sprintf(portStr,"%d",portArray[i]);
        cmdArgs[currInd+1] = portStr;
        currInd+=2;
    }

    cmdArgs[currInd] = NULL;
    
    int childPids[MAX];
    int childPidIndex = 0;

    struct sockaddr_in servAddr;

    int nodeIndex = 0;
    int nodeIdIndex = nodeId;

    if((sfd=socket(AF_INET,SOCK_STREAM,0))<0){
        perror("sockfd error");
        exit(0);
    }

    int enable = 1;
    if(setsockopt(sfd,SOL_SOCKET,SO_REUSEADDR,&enable,sizeof(int))<0){
        perror("setsockopt error");
    }

    bzero(&servAddr,sizeof(servAddr));
    servAddr.sin_family = AF_INET;
    servAddr.sin_addr.s_addr = htonl(INADDR_ANY);
    servAddr.sin_port = htons(portArray[0]);

    if((bind(sfd,(struct sockaddr*)&servAddr,sizeof(struct sockaddr_in)))<0){
        perror("bind error");
        exit(0);
    }

    if(listen(sfd,10)<0){
        perror("listen error");
        exit(0);
    }

    listenSock = sfd;

    for(int i = N-1;i>0;i--){

        if((sfd2=socket(AF_INET,SOCK_STREAM,0))<0){
            perror("sockfd error");
            exit(0);
        }

        enable = 1;
        if(setsockopt(sfd2,SOL_SOCKET,SO_REUSEPORT,&enable,sizeof(int))<0){
            perror("setsockopt error");
        }

        bzero(&servAddr,sizeof(servAddr));
        servAddr.sin_family = AF_INET;
        servAddr.sin_addr.s_addr = htonl(INADDR_ANY);
        servAddr.sin_port = htons(portArray[i]);

        if((bind(sfd2,(struct sockaddr*)&servAddr,sizeof(struct sockaddr_in)))<0){
            perror("bind error");
            exit(0);
        }

        if(listen(sfd2,10)<0){
            perror("listen error");
            exit(0);
        }

        nodeIndex = i;
        nodeIdIndex = (nodeIdIndex-1+N)%N;

        int pid = fork();
        if(pid<0){
            perror("fork");
            exit(0);
        }
        if(pid){
            childPids[childPidIndex] = pid;
            childPidIndex++;
            // Closing the socket at the parent only.
            close(sfd2);
            sfd2 = -1;
        }
        else{
            close(sfd);
            sfd = -1;

            char buff1[10],buff12[10],buff2[10],buff3[10];
            sprintf(buff1,"%d",nodeIndex);
            sprintf(buff12,"%d",nodeIdIndex);
            sprintf(buff2,"%d",N);
            sprintf(buff3,"%d",sfd2);
            cmdArgs[0] = buff1;
            cmdArgs[1] = buff12;
            cmdArgs[2] = buff2;
            cmdArgs[3] = buff3;
            execv("./node",cmdArgs);
        }
        
    }
    nodeIndex = 0;

    int* arr = (int*)malloc(sizeof(int)*N);
    FILE* fstream = fopen(argv[3],"r");
    
    if(fstream==NULL){
        printf("Input file numbers.txt does not exist.\n");
        killAllChildren(childPids,childPidIndex-1);
        exit(0);
    }

    printf("Node %d(Coordinator): Unsorted array: ",nodeId);
    for(int i = 0;i<N;i++){
        fscanf(fstream,"%d",(arr+i));
        printf("%d ",arr[i]);
    }
    printf("\n");

    fclose(fstream);

    int res = nodeMain(nodeIndex,N,arr,sfd,nodeId,ipArray,portArray);

    printf("Node %d(Coordinator): Sorted array: ",nodeId);
    for(int i = 0;i<N;i++){
        printf("%d ",arr[i]);
    }
    printf("\n");

    FILE* opFile = getOpFile("output.txt");
    
    if(opFile==NULL){
        printf("Cannot open output file.\n");
        killAllChildren(childPids,childPidIndex-1);
        if(listenSock!=-1){
            if(close(listenSock)<0){
                perror("close listenSock");
                exit(0);
            }
        }
        exit(0);
    }

    for(int i = 0;i<N;i++){
        fprintf(opFile,"%d ",arr[i]);
    }
    fprintf(opFile,"\n");

    fclose(opFile);

    printf("Coordinator: Sorted array written into file output.txt.\n");

    killAllChildren(childPids,childPidIndex-1);
    if(listenSock!=-1){
        if(close(listenSock)<0){
            perror("close listenSock");
            exit(0);
        }
    }
    exit(0); 

}
