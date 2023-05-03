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

struct Request {
    std::string request_id;
    int type;
    std::string command;
    std::string data;
    unsigned long long ts;
};

struct IpPortCmp {
    bool operator() (std::string a, std::string b) const {
        unsigned long h1 = get_hash(a);
        unsigned long h2 = get_hash(b);
        return a < b;
    }
};

int connect(std::string ip, int port, bool blocking=false) {
    struct sockaddr_in saddr;
    int fd, ret_val, ret;
    struct hostent *local_host;

    fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP); 

    if (fd == -1) {
        perror ("Creating socket failed");
        return -1;
    }

    printf("Created a socket with fd: %d\n", fd);

    saddr.sin_family = AF_INET;         
    saddr.sin_port = htons(port);     
    local_host = gethostbyname(ip.c_str());
    saddr.sin_addr = *((struct in_addr *)local_host->h_addr);

    if (!blocking) fcntl(fd, F_SETFL, O_NONBLOCK);

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
    return fd;
}

int send_to_socket(int fd, std::string msg) {
    const char *msg_chr = msg.c_str();
    unsigned long sz = strlen(msg_chr);
    long n_bytes = 0;

    while (n_bytes < sz) {
        unsigned long s = sz-n_bytes;
        s = (s > DATA_BUFFER) ? DATA_BUFFER:s;

        long n = send(fd, msg_chr+n_bytes, s, 0);
        if (n > 0) {
            n_bytes += n;
        }
        else {
            if (n_bytes < strlen(msg_chr)) {
                return -1;
            }
            else break;
        }
    }
    return 1;
}

int recv_from_socket(int fd, std::vector<std::string> &msgs, std::string sep="<EOM>") {
    std::string remainder = "";

    while (1) {
        char buf[DATA_BUFFER];
        int ret_data = recv(fd, buf, DATA_BUFFER, 0);

        if (ret_data > 0) {
            std::string msg(buf, buf + ret_data);
            std::cout << msg << std::endl;

            msg = remainder + msg;

            std::vector<std::string> parts = split_inline(msg, sep);
            msgs.insert(msgs.end(), parts.begin(), parts.end());
            remainder = msg;
        } 
        else {
            break;
        }
    }

    return 1;
}

int deserialize_msg(std::string msg, Request &out) {
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

    out = {request_id, type, command, data, ts};
    return 1;
}

struct IpPort {
    std::string ip;
    int port;
};

IpPort get_ip_port(std::string ip_port_str) {
    std::vector<std::string> addr = split(ip_port_str, ":");
    IpPort out = {addr[0], std::stoi(addr[1])};
    return out;
}

int send_message(int fd, int type, std::string cmd, std::string data, std::string request_id="", std::string ts="") {
    std::string msg;
    std::string ctime = get_current_time();
    if (request_id == "") request_id = ctime;
    if (ts == "") ts = ctime;

    msg = request_id + " " + std::to_string(type) + " " + cmd + " " + data + " " + ts + "<EOM>";
    return send_to_socket(fd, msg);
}

void add_connection(std::string ip, int port, std::unordered_map<unsigned long, int> &m) {
    int fd = connect(ip, port);
    unsigned long hash = get_hash(ip + ":" + std::to_string(port));
    m[hash] = fd;
}

void insert_kv_pair(std::string kv_pair, unsigned long long ts, std::unordered_map<std::string, HashValue> &hash_table) {
    std::vector<std::string> key_val = split(kv_pair, ":");

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
}

class Server {
    public:
    int server_fd = -1;
    int epoll_fd = -1;
    std::string public_ip;
    int public_port;
    unsigned long my_hash;
    struct epoll_event ev, events[MAX_EVENTS];
    std::unordered_map<unsigned long, int> hash_to_socket_map;
    std::unordered_map<std::string, HashValue> hash_table;
    std::unordered_map<std::string, int> request_id_to_fd;
    std::map<unsigned long, unsigned long> finger;
    std::set<std::string, IpPortCmp> successors;
    std::string predecessor="";

    void init_epoll();
    void add_fd_to_epoll(int fd, uint32_t events);
    void del_fd_from_epoll(int fd);
    void create_server();
    void run_epoll();
    int handle_request(std::string msg, int fd);
    void update_finger_table(std::string ip_port);
    int get_next_server_to_fwd(std::string key);
    void handle_get_request(std::string msg, int fd);
    void handle_put_request(std::string msg, int fd);
    void handle_join_request(std::string msg, int fd);
    void handle_update_finger_request(std::string msg, int fd);
    void handle_get_succ_request(std::string msg, int fd);
    void handle_get_pred_request(std::string msg, int fd);
    void handle_set_succ_request(std::string msg, int fd);
    void handle_set_pred_request(std::string msg, int fd);
    void handle_reconcile_keys_request(std::string msg, int fd);
};

void Server::handle_get_request(std::string msg, int fd) {
    Request req;
    int h = deserialize_msg(msg, req);

    if (h != -1) {
        std::string key = req.data;
        std::string request_id = req.request_id;
        unsigned long long ts = req.ts;

        unsigned long key_hash = get_hash(key);
        unsigned long pred_hash = get_hash(predecessor);
        unsigned long d = dist(pred_hash, my_hash);

        if ((predecessor == "") || (dist(pred_hash, key_hash) <= d && dist(key_hash, my_hash) <= d)) {
            std::string val = "<EMPTY>";

            if (hash_table.find(key) != hash_table.end()) {
                HashValue hv = hash_table[key];
                val = hv.val;
            }

            send_message(fd, 1, "GET", val, request_id, std::to_string(ts));
        }

        else {
            request_id_to_fd[request_id] = fd;

            int fwd_sock = get_next_server_to_fwd(key);
            send_message(fwd_sock, req.type, req.command, req.data, req.request_id, std::to_string(req.ts));
        }
    }
}

void Server::handle_put_request(std::string msg, int fd) {
    Request req;
    int h = deserialize_msg(msg, req);

    if (h != -1) {
        std::string data = req.data;
        std::string request_id = req.request_id;
        unsigned long long ts = req.ts;

        std::vector<std::string> key_val = split(data, ":");

        std::string key = key_val[0];
        std::string val = key_val[1];

        unsigned long key_hash = get_hash(key);
        unsigned long pred_hash = get_hash(predecessor);
        unsigned long d = dist(pred_hash, my_hash);

        if ((predecessor == "") || (dist(pred_hash, key_hash) <= d && dist(key_hash, my_hash) <= d)) {
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

            send_message(fd, 1, "PUT", "UPDATED", request_id, std::to_string(ts));
        }

        else {
            request_id_to_fd[request_id] = fd;

            int fwd_sock = get_next_server_to_fwd(key);
            send_message(fwd_sock, req.type, req.command, req.data, req.request_id, std::to_string(req.ts));
        }
    }
}

void Server::handle_join_request(std::string msg, int fd) {
    Request req;
    int h = deserialize_msg(msg, req);

    if (h != -1) {
        std::string data = req.data;
        std::string request_id = req.request_id;
        unsigned long long ts = req.ts;

        std::string ip_port = data;

        unsigned long node_hash = get_hash(ip_port);
        unsigned long pred_hash = get_hash(predecessor);
        unsigned long d = dist(pred_hash, my_hash);

        if ((predecessor == "") || (dist(pred_hash, node_hash) <= d && dist(node_hash, my_hash) <= d)) {
            send_message(fd, 1, "JOIN", public_ip + ":" + std::to_string(public_port), request_id, std::to_string(ts));
        }
        else {
            request_id_to_fd[request_id] = fd;

            int fwd_sock = get_next_server_to_fwd(ip_port);
            send_message(fwd_sock, req.type, req.command, req.data, req.request_id, std::to_string(req.ts));
        }
    }
} 

void Server::handle_update_finger_request(std::string msg, int fd) {
    Request req;
    int h = deserialize_msg(msg, req);

    if (h != -1) {
        std::string data = req.data;
        std::string request_id = req.request_id;
        unsigned long long ts = req.ts;

        std::string ip_port = data;
        unsigned long node_hash = get_hash(ip_port);

        if (node_hash != my_hash) {
            update_finger_table(ip_port);

            auto it = successors.begin();
            unsigned long next_hash = get_hash(*it);

            int fwd_sock = hash_to_socket_map[next_hash];
            send_message(fwd_sock, req.type, req.command, req.data, req.request_id, std::to_string(req.ts));
        }
    }
} 

void Server::handle_get_succ_request(std::string msg, int fd) {
    Request req;
    int h = deserialize_msg(msg, req);

    if (h != -1) {
        std::string data = req.data;
        std::string request_id = req.request_id;
        unsigned long long ts = req.ts;

        std::string msg = "";
        for (auto succ : successors) {
            msg += request_id + " 1 GET-SUCCESSOR " + succ + " " + std::to_string(ts) + "<EOM>";
        }

        send_to_socket(fd, msg);
    }
} 

void Server::handle_get_pred_request(std::string msg, int fd) {
    Request req;
    int h = deserialize_msg(msg, req);

    if (h != -1) {
        std::string data = req.data;
        std::string request_id = req.request_id;
        unsigned long long ts = req.ts;

        send_message(fd, 1, "GET-PREDECESSOR", predecessor, request_id, std::to_string(ts));
    }
} 

void Server::handle_set_succ_request(std::string msg, int fd) {
    Request req;
    int h = deserialize_msg(msg, req);

    if (h != -1) {
        std::string data = req.data;

        std::string ip_port = data;
        successors.insert(ip_port);
        update_finger_table(ip_port);
    }
} 

void Server::handle_set_pred_request(std::string msg, int fd) {
    Request req;
    int h = deserialize_msg(msg, req);

    if (h != -1) {
        std::string data = req.data;

        std::string ip_port = data;
        predecessor = ip_port;
        update_finger_table(ip_port);
    }
} 

void Server::handle_reconcile_keys_request(std::string msg, int fd) {
    Request req;
    int h = deserialize_msg(msg, req);

    if (h != -1) {
        std::string data = req.data;
        std::string request_id = req.request_id;
        unsigned long long ts = req.ts;

        std::string ip_port = data;
        unsigned long hash = get_hash(ip_port);

        if (hash_to_socket_map.find(hash) != hash_to_socket_map.end()) {
            int fd_1 = hash_to_socket_map[hash];
            std::string msg = "";

            std::vector<std::string> keys_to_delete;

            for (auto kv : hash_table) {
                std::string k = kv.first;
                HashValue hv = kv.second;
                std::string v = hv.val;
                unsigned long long ts = hv.timestamp;
                unsigned long key_hash = get_hash(k);

                if (dist(key_hash, hash) < dist(key_hash, my_hash)) {
                    std::string ctime = get_current_time();
                    msg += ctime + " 0 RECONCILE-PUT " + k + ":" + v + " " + std::to_string(ts) + "<EOM>";
                    keys_to_delete.push_back(k);
                }
            }

            for (auto k : keys_to_delete) {
                hash_table.erase(k);
            }

            send_to_socket(fd, msg);
        }
    }
} 


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

void Server::del_fd_from_epoll(int fd) {
    if (epoll_ctl (epoll_fd, EPOLL_CTL_DEL, fd, NULL) == -1) {
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
    my_hash = get_hash(ip_port);

    unsigned long p = 1;
    for (int i = 0; i < 32; i++) {
        finger[(my_hash + p) % KEY_SPACE_SIZE] = LONG_MAX;
        p *= 2;
    }

    add_fd_to_epoll(fd, EPOLLIN | EPOLLET);
}

void Server::update_finger_table(std::string ip_port) {
    unsigned long hsh = get_hash(ip_port);
    bool updated = false;

    for (auto kv : finger) {
        unsigned long key = kv.first;
        unsigned long val = kv.second;

        if (val == LONG_MAX || dist(key, val) > dist(key, hsh)) {
            finger[key] = hsh;
            updated = true;
        }
    }

    if (updated) {
        if (hash_to_socket_map.find(hsh) != hash_to_socket_map.end() && predecessor != ip_port && successors.find(ip_port) == successors.end()) {
            int fd = hash_to_socket_map[hsh];
            del_fd_from_epoll(fd);
            close(fd);
            hash_to_socket_map.erase(hsh);
        }

        IpPort i_p = get_ip_port(ip_port);
        add_connection(i_p.ip, i_p.port, hash_to_socket_map);
    }
}

void Server::run_epoll() {
    struct sockaddr_in new_addr;
    int addrlen = sizeof(struct sockaddr_in);

    while (1) {
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
                std::vector<std::string> msgs;
                recv_from_socket(fd, msgs);

                for (std::string msg : msgs) {
                    handle_request(msg, fd);
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

int Server::handle_request(std::string msg, int fd){
    Request req;
    int h = deserialize_msg(msg, req);

    if (h != -1) {
        std::string request_id = req.request_id;
        int type = req.type;
        std::string command = req.command;
        std::string data = req.data;
        unsigned long long ts = req.ts;

        std::cout << request_id << " " << type << " " << command << " " << data << " " << ts << std::endl;

        if (type == 1) {
            if (request_id_to_fd.find(request_id) != request_id_to_fd.end()) {
                int fd = request_id_to_fd[request_id];
                send_to_socket(fd, msg);
            }
            else {
                std::cout << "Could not find request id for forwarding" << std::endl;
                return -1;
            }
        }
        else {
            if (command == "PUT") {
                handle_put_request(msg, fd);
            }
            else if (command == "GET") {
                handle_get_request(msg, fd);
            }
            else if (command == "JOIN") {
                handle_join_request(msg, fd);
            }
            else if (command == "UPDATE-FINGER") {
                handle_update_finger_request(msg, fd);
            }
            else if (command == "GET-SUCCESSOR") {
                handle_get_succ_request(msg, fd);
            }
            else if (command == "GET-PREDECESSOR") {
                handle_get_pred_request(msg, fd);
            }
            else if (command == "SET-SUCCESSOR") {
                handle_set_succ_request(msg, fd);
            }
            else if (command == "SET-PREDECESSOR") {
                handle_set_pred_request(msg, fd);
            }
            else if (command == "RECONCILE-KEYS") {
                handle_reconcile_keys_request(msg, fd);
            }
        }
    }

    return 1;
}

int main (int argc, char *argv[]) {
    Server server;

    // init epoll
    server.init_epoll();
    server.predecessor = "";

    if (argc < 2) {
        perror("./server <ip> <port> <ip_port1>(optional)");
        exit(EXIT_FAILURE);
    }

    // create server
    server.public_ip = argv[1];
    server.public_port = atoi(argv[2]);
    server.create_server();

    std::string ip_port_str = server.public_ip + ":" + std::to_string(server.public_port);
    
    /* GET SUCCESSOR IN THE CIRCLE */
    
    if (argc > 3) {
        // connect with one of random servers in the circle
        std::string p(argv[3], argv[3] + strlen(argv[3]));

        // get ip and port
        IpPort i_p = get_ip_port(p);

        // connect to random server
        int fd = connect(i_p.ip, i_p.port, true);

        // send JOIN message
        int h = send_message(fd, 0, "JOIN", ip_port_str);

        if (h != -1) {
            std::vector<std::string> resp;
            h = recv_from_socket(fd, resp);

            if (h != -1) {
                // get the 1st successor
                std::string r = resp[0];
                std::cout << r << std::endl;
                Request req;
                h = deserialize_msg(r, req);

                if (h != -1) {
                    // get successor ip port
                    std::string succ_ip_port = req.data;
                    std::cout << "SUCCESSOR : " << succ_ip_port << std::endl;
                    server.successors.insert(succ_ip_port);

                    i_p = get_ip_port(succ_ip_port);

                    // add new connection to successor
                    add_connection(i_p.ip, i_p.port, server.hash_to_socket_map);
                }
            }
        }

        close(fd);
    }

    /* ASK SUCCESSOR TO UPDATE ITS PREDECESSOR AFTER IT JOINS, GET SUCCESSOR
    LIST OF SUCCESSOR AND SEND UPDATE-FINGER MSG AROUND THE CIRCLE */

    if (server.successors.size() > 0) {
        auto it = server.successors.begin();
        std::string succ_ip_port = *it;

        // connect to successor using existing fd
        unsigned long hash = get_hash(succ_ip_port);
        int fd = server.hash_to_socket_map[hash];

        // send message to get predecessor of successor
        int h = send_message(fd, 0, "GET-PREDECESSOR", "NULL");

        if (h != -1) {
            std::vector<std::string> resp;
            h = recv_from_socket(fd, resp);

            if (h != -1) {
                std::string r = resp[0];
                Request req;
                h = deserialize_msg(r, req);

                if (h != -1) {
                    // get the predecessor
                    std::string pred_ip_port = req.data;
                    std::cout << "PREDECESSOR : " << pred_ip_port << std::endl;
                    server.predecessor = pred_ip_port;

                    IpPort i_p = get_ip_port(succ_ip_port);
                    add_connection(i_p.ip, i_p.port, server.hash_to_socket_map);
                }
            }
        }

        // ask successor to set this as the predecessor
        send_message(fd, 0, "SET-PREDECESSOR", ip_port_str);

        // get successor list of its successor
        h = send_message(fd, 0, "GET-SUCCESSOR", ip_port_str);

        if (h != -1) {
            std::vector<std::string> resp;
            h = recv_from_socket(fd, resp);

            if (h != -1) {
                // add each successor to its own list of successors and make new connection
                for (std::string r : resp) {
                    Request req;
                    h = deserialize_msg(r, req);

                    if (h != -1) {
                        std::string s_ip_port = req.data;
                        std::cout << "SUCCESSOR LIST : " << s_ip_port << std::endl;
                        server.successors.insert(s_ip_port);

                        IpPort i_p = get_ip_port(s_ip_port);
                        add_connection(i_p.ip, i_p.port, server.hash_to_socket_map);
                    }
                }

                // remove the last successor because successor list of successor
                // will have R successors. It is adding own successor as one of 
                // the successor, thus it needs R-1 remaining.
                auto it = server.successors.rbegin();
                server.successors.erase(*it);
            }
        }

        // ask successor to update its finger table and pass on to its own successor
        send_message(fd, 0, "UPDATE-FINGER", ip_port_str);
    }

    /* ASK PREDECESSOR TO UPDATE ITS SUCCESSOR AFTER IT JOINS */

    if (server.predecessor != "") {
        std::string pred_ip_port = server.predecessor;

        // connect to predecessor using existing fd
        unsigned long hash = get_hash(pred_ip_port);
        int fd = server.hash_to_socket_map[hash];

        // ask predecessor to set this as the successor
        send_message(fd, 0, "SET-SUCCESSOR", ip_port_str);
    }

    /* RECONCILE KEYS FROM SUCCESSOR */

    if (server.successors.size() > 0) {
        auto it = server.successors.begin();
        std::string succ_ip_port = *it;

        // connect to successor using existing fd
        unsigned long hash = get_hash(succ_ip_port);
        int fd = server.hash_to_socket_map[hash];

        // send message to reconcile keys from successor
        int h = send_message(fd, 0, "RECONCILE-KEYS", ip_port_str);

        if (h != -1) {
            std::vector<std::string> resp;
            h = recv_from_socket(fd, resp);

            if (h != -1) {
                // update own hash table with reconciled keys from successor
                for (std::string r : resp) {
                    Request req;
                    h = deserialize_msg(r, req);

                    if (h != -1) {
                        std::string kv_pair = req.data;
                        std::cout << "KEY VALUES : " << kv_pair << std::endl;

                        unsigned long long ts = req.ts;
                        insert_kv_pair(kv_pair, ts, server.hash_table);
                    }
                }
            }
        }
    }

    server.run_epoll();
}