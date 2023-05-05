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

// Generate random string of max_length
std::string generate(int max_length=5){
    std::string possible_characters = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    std::random_device rd;
    std::mt19937 engine(rd());
    std::uniform_int_distribution<> dist(0, possible_characters.size()-1);
    
    std::string out = "";

    for(int i = 0; i < max_length; i++){
        int random_index = dist(engine); //get index between 0 and possible_characters.size()-1
        out += possible_characters[random_index];
    }

    return out;
}

unsigned long dist(unsigned long a, unsigned long b) {
    if (b < a) {
        return KEY_SPACE_SIZE + b - a;
    }
    return b - a;
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

struct HashPair {
    unsigned long ref_hash;
    std::string inp;
};

struct IpPortCmp {
    bool operator() (HashPair a, HashPair b) const {
        unsigned long h1 = get_hash(a.inp);
        unsigned long h2 = get_hash(b.inp);

        unsigned long d1 = dist(a.ref_hash, h1);
        unsigned long d2 = dist(b.ref_hash, h2);
        
        return d1 < d2;
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
    send(fd, msg_chr, strlen(msg_chr), 0);

    return 1;
}

int recv_from_socket(int fd, std::vector<std::string> &msgs, std::string sep="<EOM>") {
    std::string remainder = "";

    while (1) {
        char buf[DATA_BUFFER];
        int ret_data = recv(fd, buf, DATA_BUFFER, 0);

        if (ret_data > 0) {
            std::string msg(buf, buf + ret_data);
            // std::cout << msg << std::endl;

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

int recv_from_epoll(int fd, std::vector<std::string> &msgs, std::string sep="<EOM>") {
    int epoll_fd = epoll_create1 (0);

    if (epoll_fd == -1) {
        perror ("epoll_create");
        exit(EXIT_FAILURE);
    }

    struct epoll_event ev, events[MAX_EVENTS];

    ev.data.fd = fd;
    ev.events = EPOLLIN | EPOLLET;
    
    if (epoll_ctl (epoll_fd, EPOLL_CTL_ADD, fd, &ev) == -1) {
        perror ("epoll_ctl");
        exit(EXIT_FAILURE);
    }

    while (1) {
        int nfds = epoll_wait(epoll_fd, events, MAX_EVENTS, 50);

        if (nfds == -1) {
            perror("epoll_wait");
            exit(EXIT_FAILURE);
        }

        bool f = false;

        for (int i = 0; i < nfds; i++) {
            int fd_wdata = events[i].data.fd;

            if (fd == fd_wdata && (events[i].events & EPOLLIN)) {
                recv_from_socket(fd, msgs);
                f = true;
            }
        }

        if (f) break;
        
    }

    close(epoll_fd);

    return 1;
}

int deserialize_msg(std::string msg, Request &out) {
    std::vector<std::string> msg_parts = split(msg, " ");

    if (msg_parts.size() != 5) {
        std::cout << "Invalid request format" << std::endl;
        std::cout << msg << std::endl;
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
    std::string rand_str = generate(5);

    if (request_id == "") request_id = ctime + rand_str;
    if (ts == "") ts = ctime;

    msg = request_id + " " + std::to_string(type) + " " + cmd + " " + data + " " + ts + "<EOM>";
    return send_to_socket(fd, msg);
}

int add_connection(std::string ip, int port, std::unordered_map<unsigned long, int> &m) {
    int fd = connect(ip, port);
    unsigned long hash = get_hash(ip + ":" + std::to_string(port));
    m[hash] = fd;
    return fd;
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
    std::set<HashPair, IpPortCmp> successors;
    std::string predecessor="";
    bool has_joined = false;
    std::string random_ip;
    int random_port;
    int random_node_fd;

    void init_epoll();
    void add_fd_to_epoll(int fd, uint32_t events);
    void del_fd_from_epoll(int fd);
    void create_server();
    void run_epoll();
    int handle_request(std::string msg, int fd);
    void update_finger_table(std::string ip_port);
    int get_next_server_to_fwd(std::string key);
    int get_next_server_to_fwd(unsigned long hash);
    void handle_get_request(std::string msg, int fd);
    void handle_put_request(std::string msg, int fd);
    void handle_join_request(std::string msg, int fd);
    void handle_finger_request(std::string msg, int fd);
    void handle_update_finger_request(std::string msg, int fd);
    void handle_get_succ_request(std::string msg, int fd);
    void handle_get_pred_request(std::string msg, int fd);
    void handle_set_succ_request(std::string msg, int fd);
    void handle_set_pred_request(std::string msg, int fd);
    void handle_reconcile_keys_request(std::string msg, int fd);
    void join_chord();
    void recv_join_from_successor(std::string msg, int fd);
    void recv_finger_from_successor(std::string msg, int fd);
    void get_predecessor();
    void recv_pred_from_successor(std::string msg, int fd);
    void get_successors();
    void recv_succ_from_successor(std::string msg, int fd);
    void recv_succ_from_predecessor(std::string msg, int fd);
    void set_predecessor();
    void set_successor();
    void reconcile_keys_successor();
    void recv_keys_successor(std::string msg, int fd);
    void update_chord_fingers();
};

void Server::recv_join_from_successor(std::string msg, int fd) {
    Request req;
    int h = deserialize_msg(msg, req);

    if (h != -1) {
        std::string succ_ip_port = req.data;
        std::cout << "SUCCESSOR : " << succ_ip_port << std::endl;
        HashPair hp = {my_hash, succ_ip_port};
        successors.insert(hp);

        IpPort i_p = get_ip_port(succ_ip_port);

        // add new connection to successor
        int fd = add_connection(i_p.ip, i_p.port, hash_to_socket_map);
        add_fd_to_epoll(fd, EPOLLIN | EPOLLET | EPOLLOUT);

        send_message(fd, 0, "GET-PREDECESSOR", "NULL");
        send_message(fd, 0, "GET-SUCCESSOR", "NULL");
        send_message(fd, 0, "SET-PREDECESSOR", public_ip + ":" + std::to_string(public_port));
    }
}

void Server::recv_finger_from_successor(std::string msg, int fd) {
    Request req;
    int h = deserialize_msg(msg, req);

    if (h != -1) {
        std::vector<std::string> parts = split(req.data, "-");
        unsigned long key = std::stoul(parts[0]);
        std::string val_ip_port = parts[1];
        unsigned long val = get_hash(val_ip_port);

        if (val == my_hash) {
            auto it = successors.begin();
            std::string succ_ip_port = (*it).inp;
            val = get_hash(succ_ip_port);
        }

        IpPort i_p = get_ip_port(val_ip_port);
        finger[key] = val;

        if (hash_to_socket_map.find(val) == hash_to_socket_map.end()) {
            // add new connection to successor
            int fd = add_connection(i_p.ip, i_p.port, hash_to_socket_map);
            add_fd_to_epoll(fd, EPOLLIN | EPOLLET | EPOLLOUT);
        }
    }
}

void Server::recv_pred_from_successor(std::string msg, int fd) {
    Request req;
    int h = deserialize_msg(msg, req);

    if (h != -1) {
        std::string pred_ip_port = req.data;

        if (pred_ip_port == "NULL") {
            auto it = successors.begin();
            std::string succ_ip_port = (*it).inp;
            predecessor = succ_ip_port;

            unsigned long hash = get_hash(succ_ip_port);
            fd = hash_to_socket_map[hash];
        }
        else {
            predecessor = pred_ip_port;
            IpPort i_p = get_ip_port(pred_ip_port);

            fd = add_connection(i_p.ip, i_p.port, hash_to_socket_map);
            add_fd_to_epoll(fd, EPOLLIN | EPOLLET | EPOLLOUT);
        }

        send_message(fd, 0, "SET-SUCCESSOR", public_ip + ":" + std::to_string(public_port));
        std::cout << "PREDECESSOR : " << predecessor << std::endl;
    }
}

void Server::recv_succ_from_successor(std::string msg, int fd) {
    Request req;
    int h = deserialize_msg(msg, req);

    if (h != -1) {
        std::string s_ip_port = req.data;
        HashPair hp = {my_hash, s_ip_port};

        if (s_ip_port != "NULL" && get_hash(s_ip_port) != my_hash && successors.find(hp) == successors.end()) {
            bool added = false;

            if (successors.size() < 32) {
                successors.insert(hp);
                added = true;
            }
            else {
                auto it = successors.rbegin();
                unsigned long end_hash = get_hash((*it).inp);
                unsigned long curr_hash = get_hash(s_ip_port);

                if (dist(curr_hash, my_hash) < dist(end_hash, my_hash)) {
                    successors.erase(*it);
                    successors.insert(hp);
                    added = true;
                }
            }

            if (added) {
                IpPort i_p = get_ip_port(s_ip_port);

                int fd = add_connection(i_p.ip, i_p.port, hash_to_socket_map);
                add_fd_to_epoll(fd, EPOLLIN | EPOLLET | EPOLLOUT);
            }
        }

        // std::cout << "SUCCESSOR LIST:" << std::endl;
        // for (HashPair succ : successors) {
        //     std::cout << succ.inp << std::endl;
        // }
    }
}

void Server::recv_succ_from_predecessor(std::string msg, int fd) {
    Request req;
    int h = deserialize_msg(msg, req);

    if (h != -1) {
        auto it = successors.begin();
        std::string succ_ip_port = (*it).inp;

        unsigned long hash = get_hash(succ_ip_port);
        int fd = hash_to_socket_map[hash];

        std::string ip_port_str = public_ip + ":" + std::to_string(public_port);

        send_message(fd, 0, "UPDATE-FINGER", ip_port_str);
        send_message(fd, 0, "RECONCILE-KEYS", ip_port_str);
    }
}

void Server::recv_keys_successor(std::string msg, int fd) {
    Request req;
    int h = deserialize_msg(msg, req);

    if (h != -1) {
        std::string kv_pair = req.data;
        std::cout << "KEY VALUES : " << kv_pair << std::endl;

        unsigned long long ts = req.ts;
        insert_kv_pair(kv_pair, ts, hash_table);
    }
}

void Server::handle_get_request(std::string msg, int fd) {
    Request req;
    int h = deserialize_msg(msg, req);

    if (h != -1) {
        std::string key = req.data;
        std::string request_id = req.request_id;
        unsigned long long ts = req.ts;

        unsigned long key_hash = get_hash(key);
        unsigned long pred_hash = get_hash(predecessor);

        bool x = pred_hash <= key_hash && key_hash <= my_hash;
        bool y = key_hash <= my_hash && my_hash <= pred_hash;
        bool z = my_hash <= pred_hash && pred_hash <= key_hash;

        if ((predecessor == "") || (x || y || z)) {
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

        bool x = pred_hash <= key_hash && key_hash <= my_hash;
        bool y = key_hash <= my_hash && my_hash <= pred_hash;
        bool z = my_hash <= pred_hash && pred_hash <= key_hash;

        if ((predecessor == "") || (x || y || z)) {
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

        bool x = pred_hash <= node_hash && node_hash <= my_hash;
        bool y = node_hash <= my_hash && my_hash <= pred_hash;
        bool z = my_hash <= pred_hash && pred_hash <= node_hash;

        if ((predecessor == "") || (x || y || z)) {
            send_message(fd, 1, "JOIN", public_ip + ":" + std::to_string(public_port), request_id, std::to_string(ts));
        }
        else {
            request_id_to_fd[request_id] = fd;

            int fwd_sock = get_next_server_to_fwd(ip_port);
            send_message(fwd_sock, req.type, req.command, req.data, req.request_id, std::to_string(req.ts));
        }
    }
} 

void Server::handle_finger_request(std::string msg, int fd) {
    Request req;
    int h = deserialize_msg(msg, req);

    if (h != -1) {
        std::string data = req.data;
        std::string request_id = req.request_id;
        unsigned long long ts = req.ts;

        unsigned long node_hash = std::stoul(data);
        unsigned long pred_hash = get_hash(predecessor);

        bool x = pred_hash <= node_hash && node_hash <= my_hash;
        bool y = node_hash <= my_hash && my_hash <= pred_hash;
        bool z = my_hash <= pred_hash && pred_hash <= node_hash;

        if ((predecessor == "") || (x || y || z)) {
            send_message(fd, 1, "FINGER", std::to_string(node_hash) + "-" + public_ip + ":" + std::to_string(public_port), request_id, std::to_string(ts));
        }
        else {
            request_id_to_fd[request_id] = fd;

            int fwd_sock = get_next_server_to_fwd(node_hash);
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

            if (successors.size() > 0) {
                auto it = successors.begin();
                // std::cout << (*it).inp << std::endl;
                unsigned long next_hash = get_hash((*it).inp);

                if (next_hash != node_hash) {
                    int fwd_sock = hash_to_socket_map[next_hash];
                    send_message(fwd_sock, req.type, req.command, req.data, req.request_id, std::to_string(req.ts));
                }
            }
        }
        else {
            send_message(fd, 1, "UPDATE-FINGER", "OK", request_id, std::to_string(ts));
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
        if (successors.size() == 0) {
            msg = request_id + " 1 GET-SUCCESSOR NULL " + std::to_string(ts) + "<EOM>";
        }
        else {
            for (auto succ : successors) {
                msg += request_id + " 1 GET-SUCCESSOR " + succ.inp + " " + std::to_string(ts) + "<EOM>";
            }
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

        std::string pred = "NULL";
        if (predecessor != "") pred = predecessor;

        send_message(fd, 1, "GET-PREDECESSOR", pred, request_id, std::to_string(ts));
    }
} 

void Server::handle_set_succ_request(std::string msg, int fd) {
    Request req;
    int h = deserialize_msg(msg, req);

    if (h != -1) {
        std::string data = req.data;
        std::string request_id = req.request_id;
        unsigned long long ts = req.ts;

        std::string ip_port = data;
        HashPair hp = {my_hash, ip_port};
        successors.insert(hp);

        send_message(fd, 1, "SET-SUCCESSOR", "OK", request_id, std::to_string(ts));
    }
} 

void Server::handle_set_pred_request(std::string msg, int fd) {
    Request req;
    int h = deserialize_msg(msg, req);

    if (h != -1) {
        std::string data = req.data;
        std::string request_id = req.request_id;
        unsigned long long ts = req.ts;

        std::string ip_port = data;
        predecessor = ip_port;

        if (successors.size() == 0) {
            HashPair hp = {my_hash, ip_port};
            successors.insert(hp);
        }

        send_message(fd, 1, "SET-PREDECESSOR", "OK", request_id, std::to_string(ts));
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
                    msg += request_id + " 1 RECONCILE-KEYS " + k + ":" + v + " " + std::to_string(ts) + "<EOM>";
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
        HashPair hp = {my_hash, ip_port};

        if (hash_to_socket_map.find(hsh) != hash_to_socket_map.end() && predecessor != ip_port && successors.find(hp) == successors.end()) {
            int fd = hash_to_socket_map[hsh];
            del_fd_from_epoll(fd);
            close(fd);
            hash_to_socket_map.erase(hsh);
        }

        IpPort i_p = get_ip_port(ip_port);
        int fd = add_connection(i_p.ip, i_p.port, hash_to_socket_map);
        add_fd_to_epoll(fd, EPOLLIN | EPOLLET | EPOLLOUT);
    }
}

void Server::run_epoll() {
    struct sockaddr_in new_addr;
    int addrlen = sizeof(struct sockaddr_in);

    while (1) {
        // std::cout << "PRDEDECESSOR : " << predecessor << std::endl;
        // std::cout << "SUCCESSORS : " << std::endl;

        // for (HashPair succ : successors) {
        //     std::cout << succ.inp << std::endl;
        // }

        // std::cout << std::endl;
        // std::cout << "FINGERS : " << std::endl;

        // for (auto kv : finger) {
        //     unsigned long key = kv.first;
        //     unsigned long val = kv.second;

        //     std::cout << std::to_string(key) << ":" << std::to_string(val) << std::endl;
        // }

        // std::cout << std::endl;
        // std::cout << std::endl;

        int succ_fd = -1;

        if (successors.size() > 0) {
            auto it = successors.begin();
            std::string succ_ip_port = (*it).inp;

            unsigned long hash = get_hash(succ_ip_port);
            succ_fd = hash_to_socket_map[hash];
            send_message(succ_fd, 0, "GET-SUCCESSOR", "NULL");
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

            else if (fd == random_node_fd && (events[i].events & EPOLLOUT)) {
                send_message(fd, 0, "JOIN", public_ip + ":" + std::to_string(public_port));

                for (auto kv : finger) {
                    unsigned long key = kv.first;
                    send_message(fd, 0, "FINGER", std::to_string(key));
                }

                ev.events = EPOLLIN | EPOLLET;

                if (epoll_ctl(epoll_fd, EPOLL_CTL_MOD, random_node_fd, &ev) == -1) {
                    perror("epoll_ctl: conn_sock");
                    break;
                }
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

int Server::get_next_server_to_fwd(unsigned long hash) {
    auto it = finger.lower_bound(hash);

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

        // std::cout << request_id << " " << type << " " << command << " " << data << " " << ts << std::endl;

        if (type == 1) {
            if (request_id_to_fd.find(request_id) != request_id_to_fd.end()) {
                int fd = request_id_to_fd[request_id];
                send_message(fd, type, command, data, request_id, std::to_string(ts));
            }
            else {
                if (command == "JOIN") {
                    recv_join_from_successor(msg, fd);
                }
                else if (command == "GET-PREDECESSOR") {
                    recv_pred_from_successor(msg, fd);
                }
                else if (command == "GET-SUCCESSOR") {
                    recv_succ_from_successor(msg, fd);
                }
                else if (command == "SET-SUCCESSOR") {
                    recv_succ_from_predecessor(msg, fd);
                }
                else if (command == "RECONCILE-KEYS") {
                    recv_keys_successor(msg, fd);
                }
                else if (command == "FINGER") {
                    recv_finger_from_successor(msg, fd);
                }
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
            else if (command == "FINGER") {
                handle_finger_request(msg, fd);
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

    if (argc < 2) {
        perror("./server <ip> <port> <ip_port1>(optional)");
        exit(EXIT_FAILURE);
    }

    // create server
    server.public_ip = argv[1];
    server.public_port = atoi(argv[2]);
    server.create_server();

    if (argc > 3) {
        // connect with one of random servers in the circle
        std::string p(argv[3], argv[3] + strlen(argv[3]));

        // get ip and port
        IpPort i_p = get_ip_port(p);

        // connect to random server
        server.random_node_fd = connect(i_p.ip, i_p.port);
        server.add_fd_to_epoll(server.random_node_fd, EPOLLIN | EPOLLET | EPOLLOUT);
    }

    server.run_epoll();
}