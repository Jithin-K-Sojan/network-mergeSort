#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <strings.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <errno.h>
#include <sys/select.h>
#include <sys/types.h>
#include <signal.h>

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
struct sockaddr_in parentClAddr;

void sendElements(int clSock,int** returnArray,int nodeNum,int numChild){

    int curr = 1;
    int n = 0;
    int src = nodeNum;
    struct sockaddr_in mySockName;
    int plen = sizeof(mySockName);
    getsockname(clSock,(struct sockaddr*)&mySockName,&plen);
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

        for(int k=0;k<curr;k++){
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
        buffer[i] = htonl(buffer[i]);
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

int getParent(int nodeNum,int totalNum){
    int curr = totalNum/2;

    while(curr!=0){
        if(nodeNum%curr==0){
            return nodeNum-curr;
        }

        curr/=2;
    }
    return -1;
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

    int numEle = numOfElements(nodeNum,totalNum);
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

        
    FD_SET(servSock,&readSet);
    maxfd = servSock;
    

    FD_SET(clSock,&readSet);
    maxfd = maxfd>clSock?maxfd:clSock;

    // When reading from clSock, have to read both dest and source index.

    while(1){

        aReadSet = readSet;
        n = select(maxfd+1,&aReadSet,NULL,NULL,NULL);
        if(n<0){
            perror("select error");
            exit(0);
        }

        if(FD_ISSET(servSock,&aReadSet)){
            int dest;
            if((n = read(servSock,&dest,sizeof(int)))<0){
                perror("read error1");
                exit(0);
            }
            dest = ntohl(dest);


            if(n==0){
                FD_CLR(servSock,&readSet);
                continue;
            }

            int src;
            if(read(servSock,&src,sizeof(int))<0){
                perror("read error3");
                exit(0);
            }
            src = ntohl(src);

            struct sockaddr_in fromAddr;
            if(read(servSock,&fromAddr,sizeof(fromAddr))<0){
                perror("read error3");
                exit(0);
            }

            if(read(servSock,buffer,sizeof(int)*numOfElements(dummy(dest),totalNum))<0){
                perror("read error2");
                exit(0);
            }

            if(dummy(dest) == nodeNum){
                parentClAddr.sin_addr.s_addr = fromAddr.sin_addr.s_addr;
                parentClAddr.sin_port = fromAddr.sin_port;
                printf("Node %d(relative %d): received unsorted numbers from IP: %s PORT: %d\n",nodeId,nodeNum,inet_ntoa(parentClAddr.sin_addr),ntohs(parentClAddr.sin_port));

                valWithNode = (int)ntohl(buffer[0]);
                numEleRecv += 1;
                if(numChild!=0){
                    writeIntoResArray(buffer+1,returnArray,numChild,0);
                    sendElements(clSock,returnArray,nodeNum,numChild);
                }
            }
            else{
                dest = htonl(dest);
                if(write(clSock,&dest,sizeof(int))<0){
                    perror("write error");
                    exit(0);
                }
                dest = ntohl(dest);

                src = htonl(src);
                if(write(clSock,&src,sizeof(int))<0){
                    perror("write error");
                    exit(0);
                }
                src = ntohl(src);

                if(write(clSock,&fromAddr,sizeof(fromAddr))<0){
                    perror("write error");
                    exit(0);
                }

                if(write(clSock,buffer,sizeof(int)*numOfElements(dummy(dest),totalNum))<0){
                    perror("write error");
                    exit(0);
                }    
            }
            

        }

        if(FD_ISSET(clSock,&aReadSet)){
            int dest;
            if((n = read(clSock,&dest,sizeof(int)))<0){
                perror("read error3");
                exit(0);
            }

            if(n==0){
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
            dest = ntohl(dest);


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


            convHtoNArr(buffer,numEle);
            // Send output on sock.
            
            int dest;
            int src = nodeNum;

            dest = getParent(nodeNum,totalNum);

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

            if(write(servSock,buffer,sizeof(int)*numEle)<0){
                perror("write error");
                exit(0);
            }
            
            printf("Node %d(relative %d): sending sorted numbers to IP: %s PORT: %d\n",nodeId,nodeNum,inet_ntoa(parentClAddr.sin_addr),ntohs(parentClAddr.sin_port));

            numEleRecv = 0;

        }

    }

}

int main(int argc, char* argv[]){

    int res;
    int N = atoi(argv[2]);

    int currInd = 4;
    for(int i = 0;i<N;i++){
        ipArray[i] = argv[currInd];
        portArray[i] = atoi(argv[currInd+1]);
        currInd+=2;
    }
    res = nodeMain(atoi(argv[0]),atoi(argv[2]),NULL,atoi(argv[3]),atoi(argv[1]),ipArray,portArray);

    exit(0);
    
}