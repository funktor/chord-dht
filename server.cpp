#include <netinet/in.h> 
#include <unistd.h>
#include <stdio.h>
#include <errno.h>
#include <string>
#include <iostream>
#include <vector>
#include <algorithm>
#include <array>
#include <map>
#include <unordered_map>
#include <bits/stdc++.h>
#include <deque>
#include <tuple>
#include <map>
#include <thread>
#include <fcntl.h>
#include <sys/epoll.h>
#include <netinet/in.h> 
#include <netdb.h>
#include <functional>

#define MAX_CONNS 10000
#define KEY_SPACE_SIZE 4294967296
#define MAX_CONCURRENT_CONNECTIONS 1000
#define MAX_EVENTS 1000
#define DATA_BUFFER 1024

int close_socket(int fd) {
    close(fd);
    return -1;
}

std::vector<std::string> split_inline(std::string &s, std::string delimiter) {
    size_t pos = 0;
    std::vector<std::string> parts;

    while ((pos = s.find(delimiter)) != std::string::npos) {
        std::string token = s.substr(0, pos);
        if (token.size() > 0) parts.push_back(token);
        s.erase(0, pos + delimiter.length());
    }
    
    return parts;
}

std::vector<std::string> split(std::string s, std::string delimiter) {
    size_t pos = 0;
    std::vector<std::string> parts;

    while ((pos = s.find(delimiter)) != std::string::npos) {
        std::string token = s.substr(0, pos);
        if (token.size() > 0) parts.push_back(token);
        s.erase(0, pos + delimiter.length());
    }

    parts.push_back(s);
    
    return parts;
}

unsigned long get_hash(std::string key) {
    const std::hash<std::string> hasher;
    const auto hashResult = hasher(key);
    return hashResult % KEY_SPACE_SIZE;
}

std::string get_current_time() {
    std::time_t t = std::time(0);
    return std::to_string(t);
}

unsigned long dist(unsigned long a, unsigned long b) {
    unsigned long c = b-a;
    if (c < 0) {
        return KEY_SPACE_SIZE + c;
    }
    return c;
}

struct HashValue {
    std::string val;
    unsigned long long timestamp;
};

struct Qmsg {
    std::string msg;
    int fd;
};

class Server {
    public:
    int server_fd = -1;
    int epoll_fd = -1;
    std::string public_ip;
    int public_port;
    struct epoll_event ev, events[MAX_EVENTS];
    std::unordered_map<unsigned long, int> hash_to_socket_map;
    std::unordered_map<std::string, HashValue> hash_table;
    std::deque<Qmsg> msgs_to_send;
    std::unordered_map<std::string, int> request_id_to_fd;
    std::map<unsigned long, unsigned long> finger;
    std::set<unsigned long> partitions_hashes;

    void init_epoll();
    void add_fd_to_epoll(int fd, uint32_t events);
    void create_server();
    void run_epoll();
    int handle_request(std::string msg, int fd);
    int add_new_partition(std::string ip_port);
    void delete_partition(std::string ip_port);
    int get_next_server_to_fwd(std::string key);
    int get_next_server_in_ring(std::string key, bool exclude_self);
    void reconcile_keys();
    void ask_to_join(int fd);
};

void Server::init_epoll() {
    epoll_fd = epoll_create1 (0);

    if (epoll_fd == -1) {
        perror ("epoll_create");
        exit(EXIT_FAILURE);
    }
}

void Server::add_fd_to_epoll(int fd, uint32_t events) {
    ev.data.fd = fd;
    ev.events = events;
    
    if (epoll_ctl (epoll_fd, EPOLL_CTL_ADD, fd, &ev) == -1) {
        perror ("epoll_ctl");
        exit(EXIT_FAILURE);
    }
}

void Server::create_server() {
    struct sockaddr_in saddr;
    int fd, ret_val;
    const int opt = 1;

    fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);

    if (fd == -1) {
        printf("Could not create socket");
    }

    printf("Created a socket with fd: %d\n", fd);

    if(setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (char *)&opt, sizeof(opt)) == -1) {  
        printf("Could not set socket options");
        close_socket(fd);
    } 

    saddr.sin_family = AF_INET;         
    saddr.sin_port = htons(public_port);     
    saddr.sin_addr.s_addr = INADDR_ANY; 

    if ((ret_val = bind(fd, (struct sockaddr *)&saddr, sizeof(struct sockaddr_in))) == -1) {
        printf("Could not bind to socket");
        close_socket(fd);
    }

    if ((ret_val = listen(fd, MAX_CONCURRENT_CONNECTIONS)) == -1) {
        printf("Could not listen on socket");
        close_socket(fd);
    }

    server_fd = fd;
    fcntl(server_fd, F_SETFL, O_NONBLOCK);

    std::string ip_port = public_ip + ":" + std::to_string(public_port);
    unsigned long hsh = get_hash(ip_port);

    partitions_hashes.insert(hsh);

    unsigned long p = 1;
    for (int i = 0; i < 32; i++) {
        finger[(hsh + p) % KEY_SPACE_SIZE] = LONG_MAX;
        p *= 2;
    }

    hash_to_socket_map[hsh] = fd;
    add_fd_to_epoll(fd, EPOLLIN | EPOLLET);
}

int Server::add_new_partition(std::string ip_port) {
    std::vector<std::string> addr = split(ip_port, ":");

    if (addr.size() != 2) {
        perror ("Invalid IP PORT format, required IP:PORT");
        exit(EXIT_FAILURE);
    }

    unsigned long hsh = get_hash(ip_port);
    partitions_hashes.insert(hsh);

    for (auto kv : finger) {
        unsigned long key = kv.first;
        unsigned long val = kv.second;

        if (val == LONG_MAX || dist(key, val) > dist(key, hsh)) {
            finger[key] = hsh;
        }
    }

    std::string ip = addr[0];
    int port = std::stoi(addr[1]);

    struct sockaddr_in saddr;
    int fd, ret_val, ret;
    struct hostent *local_host;

    fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP); 

    if (fd == -1) {
        perror ("Creating socket failed");
        exit(EXIT_FAILURE);
    }

    printf("Created a socket with fd: %d\n", fd);

    saddr.sin_family = AF_INET;         
    saddr.sin_port = htons(port);     
    local_host = gethostbyname(ip.c_str());
    saddr.sin_addr = *((struct in_addr *)local_host->h_addr);

    fcntl(fd, F_SETFL, O_NONBLOCK);

    while (1) {
        ret_val = connect(fd, (struct sockaddr *)&saddr, sizeof(struct sockaddr_in));

        if (ret_val < 0) {
            std::cout << "Connect to socket failed" << std::endl;
            sleep(1);
        }
        else {
            break;
        }
    }

    printf("The Socket is now connected\n");

    hash_to_socket_map[hsh] = fd;
    add_fd_to_epoll(fd, EPOLLIN | EPOLLOUT | EPOLLET);

    return fd;
}

void Server::delete_partition(std::string ip_port) {
    std::vector<std::string> addr = split(ip_port, ":");

    if (addr.size() != 2) {
        perror ("Invalid IP PORT format, required IP:PORT");
        exit(EXIT_FAILURE);
    }

    unsigned long hsh = get_hash(ip_port);

    for (auto kv : finger) {
        unsigned long key = kv.first;
        unsigned long val = kv.second;

        auto it = partitions_hashes.upper_bound(key);

        if (it == partitions_hashes.end()) it = partitions_hashes.begin();
        finger[key] = *it;

        if (val == LONG_MAX || dist(key, val) > dist(key, hsh)) {
            finger[key] = hsh;
        }
    }

    if (partitions_hashes.find(hsh) != partitions_hashes.end()) {
        partitions_hashes.erase(hsh);
    }

    if (hash_to_socket_map.find(hsh) != hash_to_socket_map.end()) {
        int fd = hash_to_socket_map[hsh];
        close(fd);
        hash_to_socket_map.erase(hsh);
    }
}

void Server::ask_to_join(int fd) {
    std::string ctime = get_current_time();
    std::string msg = ctime + " 0 JOIN " + public_ip + ":" + std::to_string(public_port) + " " + ctime + "<EOM>";
    const char *msg_chr = msg.c_str();
    send(fd, msg_chr, strlen(msg_chr), 0);
}

void Server::reconcile_keys() {
    std::string ip_port = public_ip + ":" + std::to_string(public_port);
    int fd = get_next_server_in_ring(ip_port, true);

    if (fd == -1 || fd == server_fd) {
        perror ("Invalid server found for reconciliation");
        exit(EXIT_FAILURE);
    }

    std::string ctime = get_current_time();
    std::string msg = ctime + " 0 RECONCILE " + ip_port + " " + ctime + "<EOM>";
    const char *msg_chr = msg.c_str();
    send(fd, msg_chr, strlen(msg_chr), 0);
}

void Server::run_epoll() {
    struct sockaddr_in new_addr;
    int addrlen = sizeof(struct sockaddr_in);

    while (1) {
        while (msgs_to_send.size() > 0) {
            auto it = msgs_to_send.begin();
            Qmsg qmsg = *it;
            std::string msg = qmsg.msg;

            if (msg.size() > 0) {
                int fd = qmsg.fd;
                msg += "<EOM>";
                std::cout << "To send : " << fd << " - " << msg << std::endl;
                const char *msg_chr = msg.c_str();
                send(fd, msg_chr, strlen(msg_chr), 0);
            }
            
            msgs_to_send.pop_front();
        }

        int nfds = epoll_wait(epoll_fd, events, MAX_EVENTS, 10);

        if (nfds == -1) {
            perror("epoll_wait");
            exit(EXIT_FAILURE);
        }

        for (int i = 0; i < nfds; i++) {
            int fd = events[i].data.fd;

            if (fd == server_fd) {
                while (1) {
                    int conn_sock = accept(server_fd, (struct sockaddr*)&new_addr, (socklen_t*)&addrlen);
                    
                    if (conn_sock == -1) {
                        if ((errno == EAGAIN) || (errno == EWOULDBLOCK)) {
                            break;
                        }
                        else {
                            perror ("accept");
                            break;
                        }
                    }

                    fcntl(conn_sock, F_SETFL, O_NONBLOCK);

                    ev.events = EPOLLIN | EPOLLOUT | EPOLLET;
                    ev.data.fd = conn_sock;

                    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, conn_sock, &ev) == -1) {
                        perror("epoll_ctl: conn_sock");
                        break;
                    }
                }

                continue;
            } 
            
            else if ((events[i].events & EPOLLERR) || 
                            (events[i].events & EPOLLHUP)) {
                close (fd);
                continue;
            }

            else {
                std::string remainder = "";

                while (1) {
                    char buf[DATA_BUFFER];
                    int ret_data = recv(fd, buf, DATA_BUFFER, 0);

                    if (ret_data > 0) {
                        std::string msg(buf, buf + ret_data);
                        std::cout << msg << std::endl;

                        msg = remainder + msg;

                        std::vector<std::string> parts = split_inline(msg, "<EOM>");
                        remainder = msg;

                        for (int j = 0; j < parts.size(); j++) {
                            Server::handle_request(parts[j], fd);
                        }
                    } 
                    else {
                        break;
                    }
                }
            }
        }
    }
}

int Server::get_next_server_to_fwd(std::string key) {
    unsigned long key_hash = get_hash(key);
    auto it = finger.lower_bound(key_hash);

    unsigned long server_hash;

    if ((it == finger.end()) || (it == finger.begin())) {
        server_hash = finger.rbegin()->second;
    } 
    else {
        it--;
        server_hash = it->second;
    }

    if (hash_to_socket_map.find(server_hash) != hash_to_socket_map.end()) {
        return hash_to_socket_map[server_hash];
    }

    return 0;
}

int Server::get_next_server_in_ring(std::string key, bool exclude_self=false) {
    unsigned long key_hash = get_hash(key);
    std::set<unsigned long>::iterator it;

    if (exclude_self) {
        it = partitions_hashes.upper_bound(key_hash);
    }
    else {
        it = partitions_hashes.lower_bound(key_hash);
    }

    unsigned long server_hash;

    if (it == partitions_hashes.end()) {
        auto m = partitions_hashes.begin();
        server_hash = *m;
    } 
    else {
        server_hash = *it;
    }

    if (hash_to_socket_map.find(server_hash) != hash_to_socket_map.end()) {
        return hash_to_socket_map[server_hash];
    }

    return 0;
}

int Server::handle_request(std::string msg, int fd){
    const char *msg_chr = msg.c_str();
    std::vector<std::string> msg_parts = split(msg, " ");

    if (msg_parts.size() != 5) {
        std::cout << "Invalid request format" << std::endl;
        return -1;
    }

    std::string request_id = msg_parts[0];
    int type = std::stoi(msg_parts[1]);
    std::string command = msg_parts[2];
    std::string data = msg_parts[3];
    unsigned long long ts = std::stoull(msg_parts[4]);

    std::cout << request_id << " " << type << " " << command << " " << data << " " << ts << std::endl;

    if (command == "OK" || command == "KO") {
        if (request_id_to_fd.find(request_id) != request_id_to_fd.end()) {
            int fd = request_id_to_fd[request_id];
            struct Qmsg qmsg = {msg, fd};
            msgs_to_send.push_back(qmsg);
        }
        else {
            std::cout << "Could not find request id for forwarding" << std::endl;
            return -1;
        }
    }

    else if(command == "PUT") {
        std::vector<std::string> key_val = split(data, ":");

        if (key_val.size() != 2) {
            std::cout << "Invalid format of data for PUT request" << std::endl;
            return -1;
        }

        std::string key = key_val[0];
        std::string val = key_val[1];

        int fwd_sock = get_next_server_in_ring(key);
        std::cout << fwd_sock << std::endl;

        if (fwd_sock == server_fd) {
            if (hash_table.find(key) == hash_table.end()) {
                struct HashValue hv = {val, ts};
                hash_table[key] = hv;
            }

            else {
                HashValue hv = hash_table[key];
                unsigned long long ts_curr = hv.timestamp;
                if (ts > ts_curr) {
                    struct HashValue hv = {val, ts};
                    hash_table[key] = hv;
                }
            }

            std::string payload = request_id + " 1 OK KEY_INSERTED " + std::to_string(ts);
            struct Qmsg qmsg = {payload, fd};
            msgs_to_send.push_back(qmsg);
        }

        else {
            fwd_sock = get_next_server_to_fwd(key);

            request_id_to_fd[request_id] = fd;
            struct Qmsg qmsg = {msg, fwd_sock};
            msgs_to_send.push_back(qmsg);
        }
    }

    else if (command == "GET") {
        std::string key = data;
        int fwd_sock = get_next_server_in_ring(key);

        if (fwd_sock == server_fd) {
            std::string val = "<EMPTY>";

            if (hash_table.find(key) != hash_table.end()) {
                HashValue hv = hash_table[key];
                val = hv.val;
            }

            std::string payload = request_id + " 1 OK " + val + " " + std::to_string(ts);
            struct Qmsg qmsg = {payload, fd};
            msgs_to_send.push_back(qmsg);
        }

        else {
            fwd_sock = get_next_server_to_fwd(key);
            
            request_id_to_fd[request_id] = fd;
            struct Qmsg qmsg = {msg, fwd_sock};
            msgs_to_send.push_back(qmsg);
        }
    }

    else if(command == "JOIN") {
        std::string ip_port = data;

        unsigned long hsh = get_hash(ip_port);
        if (hash_to_socket_map.find(hsh) == hash_to_socket_map.end()) {
            add_new_partition(ip_port);
        }

        std::string payload = request_id + " 1 OK PARTITION_ADDED " + std::to_string(ts);
        struct Qmsg qmsg = {payload, fd};
        msgs_to_send.push_back(qmsg);
    }

    else if (command == "RECONCILE") {
        std::string ip_port = data;
        unsigned long hsh = get_hash(ip_port);

        if (hash_to_socket_map.find(hsh) != hash_to_socket_map.end()) {
            int fd_1 = hash_to_socket_map[hsh];
            std::string msg = "";

            std::vector<std::string> keys_to_delete;

            for (auto kv : hash_table) {
                std::string k = kv.first;
                HashValue hv = kv.second;
                std::string v = hv.val;
                unsigned long long ts = hv.timestamp;

                if (get_next_server_in_ring(k) == fd_1) {
                    std::string ctime = get_current_time();
                    msg += ctime + " 0 RECONCILE-PUT " + k + ":" + v + " " + std::to_string(ts) + "<EOM>";
                    keys_to_delete.push_back(k);
                }
            }

            for (auto k : keys_to_delete) {
                hash_table.erase(k);
            }

            struct Qmsg qmsg = {msg, fd};
            msgs_to_send.push_back(qmsg);
        }
    }

    else if(command == "RECONCILE-PUT") {
        std::vector<std::string> key_val = split(data, ":");

        if (key_val.size() != 2) {
            std::cout << "Invalid format of data for PUT request" << std::endl;
            return -1;
        }

        std::string key = key_val[0];
        std::string val = key_val[1];

        if (hash_table.find(key) == hash_table.end()) {
            struct HashValue hv = {val, ts};
            hash_table[key] = hv;
        }

        else {
            HashValue hv = hash_table[key];
            unsigned long long ts_curr = hv.timestamp;
            if (ts > ts_curr) {
                struct HashValue hv = {val, ts};
                hash_table[key] = hv;
            }
        }

        std::string payload = request_id + " 1 OK KEY_INSERTED " + std::to_string(ts);
        struct Qmsg qmsg = {payload, fd};
        msgs_to_send.push_back(qmsg);
    }

    return 1;
}

int main (int argc, char *argv[]) {
    Server server;
    server.init_epoll();

    if (argc < 2) {
        perror("./server <ip> <port> <ip_port1> <ip_port2> ...");
        exit(EXIT_FAILURE);
    }

    server.public_ip = argv[1];
    server.public_port = atoi(argv[2]);
    server.create_server();

    for (int i = 3; i < argc; i++) {
        std::string p(argv[i], argv[i] + strlen(argv[i]));
        int fd = server.add_new_partition(p);
        server.ask_to_join(fd);
    }

    server.reconcile_keys();
    server.run_epoll();
}