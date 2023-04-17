#include <netinet/in.h> 
#include <netdb.h>
#include <unistd.h>
#include <stdio.h>
#include <errno.h>
#include <thread>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <cstring>
#include <string>
#include <random>
#include <algorithm>
#include <chrono>
#include <iostream>
#include <unistd.h>
#include <mutex>
#include <thread>
#include <ctime> 
#include <sys/stat.h>   // stat
#include <stdbool.h>    // bool type
#include <fstream>

#define MAX_LIMIT 4096

std::string get_current_time() {
    std::time_t t = std::time(0);
    return std::to_string(t);
}

std::vector<std::string> split(std::string &s, std::string delimiter) {
    size_t pos = 0;
    std::vector<std::string> parts;

    while ((pos = s.find(delimiter)) != std::string::npos) {
        std::string token = s.substr(0, pos);
        if (token.size() > 0) parts.push_back(token);
        s.erase(0, pos + delimiter.length());
    }
    
    return parts;
}

void recv_messages(int server_fd) {
    int ret_data;
    std::string remainder = "";

    while (1) {
        char buf[MAX_LIMIT];
        ret_data = recv(server_fd, buf, MAX_LIMIT, 0);

        if (ret_data > 0) {
            std::string msg(buf, buf+ret_data);
            msg = remainder + msg;
            std::vector<std::string> parts = split(msg, "<EOM>");
            remainder = msg;
            for (int i = 0; i < parts.size(); i++) {
                std::cout << parts[i] << std::endl;
            }
        }
        else {
            remainder = "";
        }
    }
}

// Generate random string of max_length
std::string generate(int max_length){
    std::string possible_characters = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    std::random_device rd;
    std::mt19937 engine(rd());
    std::uniform_int_distribution<> dist(0, possible_characters.size()-1);
    
    std::string key = "";

    for(int i = 0; i < 3; i++){
        int random_index = dist(engine); //get index between 0 and possible_characters.size()-1
        key += possible_characters[random_index];
    }

    std::string val = "";

    for(int i = 0; i < max_length; i++){
        int random_index = dist(engine); //get index between 0 and possible_characters.size()-1
        val += possible_characters[random_index];
    }

    return key + ":" + val;
}

int main () {
    struct sockaddr_in saddr;
    int fd, ret_val, ret;
    struct hostent *local_host; /* need netdb.h for this */
    char msg[MAX_LIMIT];

    /* Step1: create a TCP socket */
    fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP); 
    if (fd == -1) {
        printf("socket failed \n");
        return -1;
    }
    printf("Created a socket with fd: %d\n", fd);

    /* Let us initialize the server address structure */
    saddr.sin_family = AF_INET;         
    saddr.sin_port = htons(5001);     
    local_host = gethostbyname("127.0.0.1");
    saddr.sin_addr = *((struct in_addr *)local_host->h_addr);

    /* Step2: connect to the TCP server socket */
    ret_val = connect(fd, (struct sockaddr *)&saddr, sizeof(struct sockaddr_in));
    if (ret_val == -1) {
        printf("connect failed");
        close(fd);
        return -1;
    }
    printf("The Socket is now connected\n");

    std::thread t(recv_messages, fd);
    int i = 0;
    while (1) {
        std::string msg_s = generate(5);
        std::cout << msg_s << std::endl;

        const char *msg = msg_s.c_str();

        // fgets(msg, MAX_LIMIT, stdin);
        std::string msg2(msg, msg + strlen(msg));
        msg2.erase(std::remove(msg2.begin(), msg2.end(), '\n'), msg2.cend());

        if (msg2.find(":") != std::string::npos) {
            msg2 = std::to_string(i) + " 0 PUT " + msg2 + " " + get_current_time() + "<EOM>";
        }
        else {
            msg2 = std::to_string(i) + " 0 GET " + msg2 + " " + get_current_time() + "<EOM>";
        }

        const char *msg_chr = msg2.c_str();
        ret_val = send(fd, msg_chr, strlen(msg_chr), 0);
        i++;
        // sleep(1);
    }

    t.join();

    /* Last step: close the socket */
    close(fd);
    return 0;
}