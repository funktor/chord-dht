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
#define MAX_BUFFER_RECV 1024*1024

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

int get_random(int a, int b) {
    int range = b - a + 1;
    return std::rand() % range + a;
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
    int res = send(fd, msg_chr, strlen(msg_chr), 0);

    return res;
}

int recv_from_socket(int fd, std::vector<std::string> &msgs, std::string sep="<EOM>") {
    std::string remainder = "";

    while (1) {
        char buf[DATA_BUFFER];
        int ret_data = recv(fd, buf, DATA_BUFFER, 0);

        if (ret_data > 0) {
            std::string msg(buf, buf + ret_data);
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

int recv_blocking(int fd, std::vector<std::string> &msgs, std::string sep="<EOM>") {
    char buf[MAX_BUFFER_RECV];
    int ret_data = recv(fd, buf, MAX_BUFFER_RECV, 0);

    if (ret_data > 0) {
        std::string msg(buf, buf + ret_data);

        std::vector<std::string> parts = split_inline(msg, sep);
        msgs.insert(msgs.end(), parts.begin(), parts.end());
        return 1;
    } 

    return -1;
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

int recv_select(int fd, std::vector<std::string> &msgs, std::string sep="<EOM>") {
    fd_set read_fd_set;
    struct timeval tv = {0, 10000};

    while (1) {
        FD_ZERO(&read_fd_set);
        FD_SET(fd, &read_fd_set);
        
        int ret_val = select(FD_SETSIZE, &read_fd_set, NULL, NULL, &tv);

        if (ret_val != -1) {
            if (FD_ISSET(fd, &read_fd_set)) {
                std::string remainder = "";
                while (1) {
                    char buf[DATA_BUFFER];
                    int ret_data = recv(fd, buf, DATA_BUFFER, 0);

                    if (ret_data > 0) {
                        std::string msg(buf, buf + ret_data);

                        msg = remainder + msg;

                        std::vector<std::string> parts = split_inline(msg, sep);
                        msgs.insert(msgs.end(), parts.begin(), parts.end());
                        remainder = msg;
                    } 
                    else {
                        break;
                    }
                }
                break;
            }
        }
    }
    
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

int send_message(int fd, std::string req_str) {
    return send_to_socket(fd, req_str);
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

std::string serialize_request(Request req) {
    return req.request_id + " " + std::to_string(req.type) + " " + req.command + " " + req.data + " " + std::to_string(req.ts) + "<EOM>";
}

int send_message(int fd, Request req) {
    std::string ser = serialize_request(req);
    return send_to_socket(fd, ser);
}

bool is_cyclic(unsigned long a, unsigned long b, unsigned long c) {
    bool x = (a < b) && (b <= c);
    bool y = (b <= c) && (c < a);
    bool z = (c < a) && (a < b);

    return x || y || z;
}

std::vector<Request> send_and_recv(int fd, Request req, bool wait_resp) {
    std::vector<Request> resp;

    std::string req_ser = serialize_request(req);
    int res = send_message(fd, req_ser);

    return resp;
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
    std::unordered_map<int, unsigned long> socket_last_received;
    bool successor_found = false;

    void init_epoll();
    void add_fd_to_epoll(int fd, uint32_t events);
    void del_fd_from_epoll(int fd);
    void create_server();
    void run_epoll();
    int handle_request(std::string msg, int fd);
    void update_finger_table(std::string ip_port);
    void update_finger_table(std::string ip_port, unsigned long diff);
    int get_next_server_to_fwd(std::string key);
    int get_next_server_to_fwd(unsigned long hash);
    int get_next_server_to_fwd_reverse(unsigned long hash);
    unsigned long get_next_server_hash_to_fwd(std::string key);
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
    IpPort get_successor_ip_port();
    

    std::vector<Request> get_predecessor(Request req);
    std::vector<Request> get_predecessor_rpc(Request req, std::string ip, int port, bool wait_resp);
    
    std::vector<Request> get_successors(Request req);
    std::vector<Request> get_successors_rpc(Request req, std::string ip, int port, bool wait_resp);

    std::vector<Request> set_predecessor(Request req);
    std::vector<Request> set_predecessor_rpc(Request req, std::string ip, int port, bool wait_resp);
    
    std::vector<Request> set_successor(Request req);
    std::vector<Request> set_successor_rpc(Request req, std::string ip, int port, bool wait_resp);

    std::vector<Request> put_kv(Request req);
    std::vector<Request> put_kv_rpc(Request req, std::string ip, int port, bool wait_resp);

    std::vector<Request> get_v(Request req);
    std::vector<Request> get_v_rpc(Request req, std::string ip, int port, bool wait_resp);

    std::vector<Request> get_finger(Request req);
    std::vector<Request> get_finger_rpc(Request req, std::string ip, int port, bool wait_resp);

    std::vector<Request> set_finger(Request req);
    std::vector<Request> set_finger_rpc(Request req, std::string ip, int port, bool wait_resp);

    std::vector<Request> set_finger_pred(Request req);
    std::vector<Request> set_finger_pred_rpc(Request req, std::string ip, int port, bool wait_resp);

    std::vector<Request> reconcile_keys(Request req);
    std::vector<Request> reconcile_keys_rpc(Request req, std::string ip, int port, bool wait_resp);

    void update_predecessor();
    void update_successor_pred();
    void update_successor_list();
    void update_predecessor_succ();
    void update_own_finger();
    void update_chord_fingers();
    void update_chord_fingers_predecessor();
    void reconcile_keys_succ();
    void insert_successor(std::string ip_port);
    void send_message_to_hash(Request req, std::string ip, int port, unsigned long hash);
};

void Server::insert_successor(std::string ip_port) {
    if (!successor_found) {
        successor_found = true;
        successors.clear();
    }

    HashPair hp = {my_hash, ip_port};

    if (successors.size() < 32) {
        successors.insert(hp);
    }
    else {
        auto it = successors.rbegin();
        unsigned long end_hash = get_hash((*it).inp);
        unsigned long curr_hash = get_hash(ip_port);

        if (dist(my_hash, curr_hash) < dist(my_hash, end_hash)) {
            successors.erase(*it);
            successors.insert(hp);
        }
    }
}

void Server::send_message_to_hash(Request req, std::string ip, int port, unsigned long hash) {
    int fd;

    if (hash_to_socket_map.find(hash) != hash_to_socket_map.end()) {
        fd = hash_to_socket_map[hash];
    }
    else {
        fd = add_connection(ip, port, hash_to_socket_map);
        add_fd_to_epoll(fd, EPOLLIN | EPOLLET | EPOLLOUT);
    }

    send_message(fd, req);
}

IpPort Server::get_successor_ip_port() {
    IpPort out = {"", -1};

    if (successors.size() > 0) {
        auto it = successors.begin();
        std::string ip_port = (*it).inp;
        IpPort i_p = get_ip_port(ip_port);
        return i_p;
    }

    return out;
}

// SET PREDECESSOR (STARTS HERE)

std::vector<Request> Server::get_predecessor(Request req) {
    std::string request_id = req.request_id;
    unsigned long long ts = req.ts;

    std::string pred;

    if (predecessor != "") pred = predecessor;
    else pred = public_ip + ":" + std::to_string(public_port);

    Request r = {request_id, 1, "GET-PREDECESSOR", pred, ts};

    std::vector<Request> resp;
    resp.push_back(r);

    return resp;
}

std::vector<Request> Server::get_predecessor_rpc(Request req, std::string ip, int port, bool wait_resp=true) {
    std::string ip_str = ip + ":" + std::to_string(port);
    unsigned long hash = get_hash(ip_str);
    std::vector<Request> resp;

    if (hash == my_hash) {
        resp = get_predecessor(req);
    }
    else {
        send_message_to_hash(req, ip, port, hash);
    }

    return resp;
}

void Server::update_predecessor() {
    IpPort i_p = get_successor_ip_port();

    if (i_p.port != -1) {
        std::string curr_time = get_current_time();
        std::string request_id = curr_time + generate(5);
    
        Request r = {request_id, 0, "GET-PREDECESSOR", "NULL", std::stoull(curr_time)};
        get_predecessor_rpc(r, i_p.ip, i_p.port);
    }
    else {
        std::cout << "Successors empty" << std::endl;
    }
}

// SET PREDECESSOR (ENDS HERE)

// SET SUCCESSOR LIST (STARTS HERE)

std::vector<Request> Server::get_successors(Request req) {
    std::string request_id = req.request_id;
    unsigned long long ts = req.ts;
    std::vector<Request> resp;

    Request r = {request_id, 1, "GET-SUCCESSOR", public_ip + ":" + std::to_string(public_port), ts};
    resp.push_back(r);

    for (auto succ : successors) {
        Request r = {request_id, 1, "GET-SUCCESSOR", succ.inp, ts};
        resp.push_back(r);
    }

    return resp;
}

std::vector<Request> Server::get_successors_rpc(Request req, std::string ip, int port, bool wait_resp=true) {
    std::string ip_str = ip + ":" + std::to_string(port);
    unsigned long hash = get_hash(ip_str);
    std::vector<Request> resp;

    if (hash == my_hash) {
        ip_str = req.data;
        hash = get_hash(ip_str);

        unsigned long pred_hash = get_hash(predecessor);

        if (predecessor == "" || is_cyclic(pred_hash, hash, my_hash) || pred_hash == hash) {
            resp = get_successors(req);
        }

        else {
            int fd = get_next_server_to_fwd(ip_str);
            if (fd == -1) {
                Request r = {req.request_id, 1, "GET-SUCCESSOR", "NULL", req.ts};
                resp.push_back(r);
            }
            else {
                if (fd != server_fd) {
                    resp = send_and_recv(fd, req, wait_resp);
                }
                else {
                    Request r = {req.request_id, 1, "GET-SUCCESSOR", "NULL", req.ts};
                    resp.push_back(r);
                }
            }
        }
    }
    else {
        send_message_to_hash(req, ip, port, hash);
    }

    return resp;
}

void Server::update_successor_list() {
    IpPort i_p = get_successor_ip_port();
    std::string ip_port_str = public_ip + ":" + std::to_string(public_port);

    if (i_p.port != -1) {
        std::string curr_time = get_current_time();
        std::string request_id = curr_time + generate(5);
    
        Request r = {request_id, 0, "GET-SUCCESSOR", ip_port_str, std::stoull(curr_time)};
        get_successors_rpc(r, i_p.ip, i_p.port);
    }
    else {
        std::cout << "Successors empty" << std::endl;
    }
}

// SET SUCCESSOR LIST (ENDS HERE)

// SET PREDECESSOR OF SUCCESSOR (STARTS HERE)

std::vector<Request> Server::set_predecessor(Request req) {
    std::string data = req.data;
    std::string request_id = req.request_id;
    unsigned long long ts = req.ts;

    std::vector<Request> resp;

    std::string ip_port = data;
    predecessor = ip_port;

    if (successors.size() == 0) {
        HashPair hp = {my_hash, ip_port};
        successors.insert(hp);
    }

    Request r = {request_id, 1, "SET-PREDECESSOR", "OK", ts};
    resp.push_back(r);

    return resp;
}

std::vector<Request> Server::set_predecessor_rpc(Request req, std::string ip, int port, bool wait_resp=true) {
    std::string ip_str = ip + ":" + std::to_string(port);
    unsigned long hash = get_hash(ip_str);
    std::vector<Request> resp;

    if (hash == my_hash) {
        resp = set_predecessor(req);
    }
    else {
        send_message_to_hash(req, ip, port, hash);
    }

    return resp;
}

void Server::update_predecessor_succ() {
    IpPort i_p = get_successor_ip_port();
    std::string ip_port_str = public_ip + ":" + std::to_string(public_port);

    if (i_p.port != -1) {
        std::string curr_time = get_current_time();
        std::string request_id = curr_time + generate(5);
    
        Request r = {request_id, 0, "SET-PREDECESSOR", ip_port_str, std::stoull(curr_time)};
        std::vector<Request> resp = set_predecessor_rpc(r, i_p.ip, i_p.port);
    }
    else {
        std::cout << "Successors empty" << std::endl;
    }
}

// SET PREDECESSOR OF SUCCESSOR (ENDS HERE)

// SET SUCCESSOR OF PREDECESSOR (STARTS HERE)

std::vector<Request> Server::set_successor(Request req) {
    std::string data = req.data;
    std::string request_id = req.request_id;
    unsigned long long ts = req.ts;

    std::vector<Request> resp;

    std::string ip_port = data;
    insert_successor(ip_port);

    Request r = {request_id, 1, "SET-SUCCESSOR", "OK", ts};
    resp.push_back(r);

    return resp;
}

std::vector<Request> Server::set_successor_rpc(Request req, std::string ip, int port, bool wait_resp=true) {
    std::string ip_str = ip + ":" + std::to_string(port);
    unsigned long hash = get_hash(ip_str);
    std::vector<Request> resp;

    if (hash == my_hash) {
        resp = set_successor(req);
    }
    else {
        send_message_to_hash(req, ip, port, hash);
    }

    return resp;
}

void Server::update_successor_pred() {
    std::string ip_port_str = public_ip + ":" + std::to_string(public_port);

    if (predecessor != "") {
        IpPort i_p = get_ip_port(predecessor);
        std::string curr_time = get_current_time();
        std::string request_id = curr_time + generate(5);
    
        Request r = {request_id, 0, "SET-SUCCESSOR", ip_port_str, std::stoull(curr_time)};
        std::vector<Request> resp = set_successor_rpc(r, i_p.ip, i_p.port);
    }
    else {
        std::cout << "Predecessor empty" << std::endl;
    }
}

// SET SUCCESSOR OF PREDECESSOR (ENDS HERE)

// PUT KEY-VALUE PAIR (STARTS HERE)

std::vector<Request> Server::put_kv(Request req) {
    std::string data = req.data;
    std::string request_id = req.request_id;
    unsigned long long ts = req.ts;

    std::vector<Request> resp;

    std::vector<std::string> key_val = split(data, ":");

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

    Request r = {request_id, 1, "PUT", "UPDATED", ts};
    resp.push_back(r);

    return resp;
}

std::vector<Request> Server::put_kv_rpc(Request req, std::string ip, int port, bool wait_resp=true) {
    std::string data = req.data;
    std::string request_id = req.request_id;
    unsigned long long ts = req.ts;

    std::vector<Request> resp;

    std::vector<std::string> key_val = split(data, ":");

    std::string key = key_val[0];
    std::string val = key_val[1];

    unsigned long key_hash = get_hash(key);
    unsigned long pred_hash = get_hash(predecessor);

    if ((predecessor == "") || is_cyclic(pred_hash, key_hash, my_hash) || pred_hash == key_hash) {
        resp = put_kv(req);
    }

    else {
        int fd = get_next_server_to_fwd(key);

        if (fd != server_fd) {
            resp = send_and_recv(fd, req, wait_resp);
        }
        else {
            resp = put_kv(req);
        }
    }

    return resp;
}

// PUT KEY-VALUE PAIR (ENDS HERE)

// GET VALUE CORRESPONDING TO KEY (STARTS HERE)

std::vector<Request> Server::get_v(Request req) {
    std::string data = req.data;
    std::string request_id = req.request_id;
    unsigned long long ts = req.ts;

    std::vector<Request> resp;

    std::string key = data;
    std::string val = "<EMPTY>";

    if (hash_table.find(key) != hash_table.end()) {
        HashValue hv = hash_table[key];
        val = hv.val;
    }

    Request r = {request_id, 1, "GET", val, ts};
    resp.push_back(r);

    return resp;
}

std::vector<Request> Server::get_v_rpc(Request req, std::string ip, int port, bool wait_resp=true) {
    std::string data = req.data;
    std::string request_id = req.request_id;
    unsigned long long ts = req.ts;

    std::vector<Request> resp;

    std::string key = data;

    unsigned long key_hash = get_hash(key);
    unsigned long pred_hash = get_hash(predecessor);

    if ((predecessor == "") || is_cyclic(pred_hash, key_hash, my_hash) || pred_hash == key_hash) {
        resp = get_v(req);
    }

    else {
        int fd = get_next_server_to_fwd(key);

        if (fd != server_fd) {
            resp = send_and_recv(fd, req, wait_resp);
        }
        else {
            resp = get_v(req);
        }
    }

    return resp;
}

// GET VALUE CORRESPONDING TO KEY (ENDS HERE)

// GET FINGER TABLE ENTRIES (STARTS HERE)

std::vector<Request> Server::get_finger(Request req) {
    std::string data = req.data;
    std::string request_id = req.request_id;
    unsigned long long ts = req.ts;

    std::vector<Request> resp;

    std::vector<std::string> parts = split(data, "-");
    unsigned long j = std::stoul(parts[0]);

    Request r = {request_id, 1, "FINGER", std::to_string(j) + "-" + public_ip + ":" + std::to_string(public_port), ts};

    resp.push_back(r);

    return resp;
}

std::vector<Request> Server::get_finger_rpc(Request req, std::string ip, int port, bool wait_resp=true) {
    std::string ip_str = ip + ":" + std::to_string(port);
    unsigned long hash = get_hash(ip_str);
    std::vector<Request> resp;

    if (hash == my_hash) {
        std::string data = req.data;
        std::string request_id = req.request_id;
        unsigned long long ts = req.ts;

        std::vector<std::string> parts = split(data, "-");
        unsigned long j = std::stoul(parts[0]);

        unsigned long node_hash = std::stoul(parts[1]);
        unsigned long pred_hash = get_hash(predecessor);
        
        if ((predecessor == "") || is_cyclic(pred_hash, node_hash, my_hash) || pred_hash == node_hash) {
            resp = get_finger(req);
        }
        else {
            int fd = get_next_server_to_fwd(node_hash);

            if (fd == -1) {
                Request r = {request_id, 1, "FINGER", "NULL", ts};
                resp.push_back(r);
            }
            else {
                if (fd != server_fd) {
                    resp = send_and_recv(fd, req, wait_resp);
                }
                else {
                    Request r = {request_id, 1, "FINGER", "NULL", ts};
                    resp.push_back(r);
                }
            }
        }
    }
    else {
        send_message_to_hash(req, ip, port, hash);
    }

    return resp;
}

void Server::update_own_finger() {
    IpPort i_p = get_successor_ip_port();
    std::string ip_port_str = public_ip + ":" + std::to_string(public_port);

    if (i_p.port != -1) {
        unsigned long p = 1;

        for (int i = 0; i < 32; i++) {
            unsigned long key = (my_hash + p) % KEY_SPACE_SIZE;

            std::string curr_time = get_current_time();
            std::string request_id = curr_time + generate(5);
        
            std::string data = std::to_string(p) + "-" + std::to_string(key);

            Request r = {request_id, 0, "FINGER", data, std::stoull(curr_time)};
            get_finger_rpc(r, i_p.ip, i_p.port);

            p *= 2;
        }
    }
    else {
        std::cout << "Successors empty" << std::endl;
    }
}

// GET FINGER TABLE ENTRIES (ENDS HERE)

// UPDATE FINGER TABLES OF CHORD (STARTS HERE)

std::vector<Request> Server::set_finger(Request req) {
    std::string data = req.data;
    std::string request_id = req.request_id;
    unsigned long long ts = req.ts;

    std::vector<Request> resp;

    std::vector<std::string> parts = split(data, "-");

    unsigned long j = std::stoul(parts[0]);
    std::string ip_port = parts[1];

    update_finger_table(ip_port, j);

    Request r = {request_id, 1, "UPDATE-FINGER", "OK", ts};
    resp.push_back(r);

    return resp;
}

std::vector<Request> Server::set_finger_rpc(Request req, std::string ip, int port, bool wait_resp=true) {
    std::string ip_str = ip + ":" + std::to_string(port);
    unsigned long hash = get_hash(ip_str);
    std::vector<Request> resp;

    if (hash == my_hash) {
        std::string data = req.data;
        std::string request_id = req.request_id;
        unsigned long long ts = req.ts;

        std::vector<std::string> parts = split(data, "-");

        unsigned long j = std::stoul(parts[0]);
        std::string ip_port = parts[1];

        unsigned long node_hash = get_hash(ip_port);
        unsigned long pred_hash = get_hash(predecessor);

        if (predecessor == "" || is_cyclic(pred_hash, (my_hash + j)%KEY_SPACE_SIZE, node_hash) || pred_hash == (my_hash + j)%KEY_SPACE_SIZE) {
            req.command = "UPDATE-FINGER-PREDECESSOR";
            resp = set_finger_pred_rpc(req, ip, port, true);
            for (int i = 0; i < resp.size(); i++) resp[i].command = "UPDATE-FINGER";
        }

        else {
            unsigned long new_hash = dist(j, node_hash);
            int fd = get_next_server_to_fwd_reverse(new_hash);

            if (fd == -1) {
                Request r = {request_id, 1, "UPDATE-FINGER", "OK", ts};
                resp.push_back(r);
            }
            else {
                if (fd != server_fd) {
                    resp = send_and_recv(fd, req, wait_resp);
                }
                else {
                    Request r = {request_id, 1, "UPDATE-FINGER", "OK", ts};
                    resp.push_back(r);
                }
            }
        }
    }
    else {
        send_message_to_hash(req, ip, port, hash);
    }

    return resp;
}

void Server::update_chord_fingers() {
    IpPort i_p = get_successor_ip_port();
    std::string ip_port_str = public_ip + ":" + std::to_string(public_port);

    if (i_p.port != -1) {
        unsigned long p = 1;

        for (int i = 0; i < 32; i++) {
            std::string data = std::to_string(p) + "-" + ip_port_str;

            std::string curr_time = get_current_time();
            std::string request_id = curr_time + generate(5);
        
            Request r = {request_id, 0, "UPDATE-FINGER", data, std::stoull(curr_time)};
            std::vector<Request> resp = set_finger_rpc(r, i_p.ip, i_p.port);

            p *= 2;
        }
    }
    else {
        std::cout << "Successors empty" << std::endl;
    }
}

// UPDATE FINGER TABLES OF CHORD (ENDS HERE)

// UPDATE FINGER TABLE OF PREDECESSOR (STARTS HERE)

std::vector<Request> Server::set_finger_pred(Request req) {
    std::string data = req.data;
    std::string request_id = req.request_id;
    unsigned long long ts = req.ts;

    std::vector<Request> resp;

    Request r = {request_id, 1, "UPDATE-FINGER-PREDECESSOR", "OK", ts};
    resp.push_back(r);

    return resp;
}

std::vector<Request> Server::set_finger_pred_rpc(Request req, std::string ip, int port, bool wait_resp=true) {
    std::string ip_str = ip + ":" + std::to_string(port);
    unsigned long hash = get_hash(ip_str);
    std::vector<Request> resp;

    if (hash == my_hash) {
        std::string data = req.data;
        std::string request_id = req.request_id;
        unsigned long long ts = req.ts;

        std::vector<std::string> parts = split(data, "-");

        unsigned long j = std::stoul(parts[0]);
        std::string ip_port = parts[1];

        unsigned long node_hash = get_hash(ip_port);
        unsigned long pred_hash = get_hash(predecessor);

        if (predecessor == "" || is_cyclic(pred_hash, (my_hash + j)%KEY_SPACE_SIZE, node_hash)) {
            update_finger_table(ip_port, j);

            if (predecessor != "") {
                IpPort i_p = get_ip_port(predecessor);
                resp = set_finger_pred_rpc(req, i_p.ip, i_p.port, true);
            }
            else {
                resp = set_finger_pred(req);
            }
        }
        else {
            resp = set_finger_pred(req);
        }
    }
    else {
        send_message_to_hash(req, ip, port, hash);
    }

    return resp;
}

// UPDATE FINGER TABLE OF PREDECESSOR (ENDS HERE)


// RECONCILE KEYS FROM SUCCESSOR (STARTS HERE)

std::vector<Request> Server::reconcile_keys(Request req) {
    std::string data = req.data;
    std::string request_id = req.request_id;
    unsigned long long ts = req.ts;

    std::vector<Request> resp;

    unsigned long hash = std::stoul(data);

    std::vector<std::string> keys_to_delete;

    for (auto kv : hash_table) {
        std::string k = kv.first;
        HashValue hv = kv.second;
        std::string v = hv.val;
        unsigned long long ts = hv.timestamp;

        unsigned long key_hash = get_hash(k);

        if (dist(key_hash, hash) < dist(key_hash, my_hash)) {
            std::string request_id = get_current_time() + generate(5);

            Request r = {request_id, 1, "RECONCILE-KEYS", k + ":" + v, ts};
            resp.push_back(r);
            keys_to_delete.push_back(k);
        }
    }

    for (auto k : keys_to_delete) {
        hash_table.erase(k);
    }

    return resp;
}

std::vector<Request> Server::reconcile_keys_rpc(Request req, std::string ip, int port, bool wait_resp=true) {
    std::string ip_port = ip + ":" + std::to_string(port);
    unsigned long hash = get_hash(ip_port);
    std::vector<Request> resp;

    if (hash == my_hash) {
        std::string request_id = req.request_id;
        unsigned long ts = req.ts;

        hash = std::stoul(req.data);
        unsigned long pred_hash = get_hash(predecessor);

        if (predecessor == "" || is_cyclic(pred_hash, hash, my_hash) || pred_hash == hash) {
            resp = reconcile_keys(req);
        }
        else {
            int fd = get_next_server_to_fwd(hash);

            if (fd == -1) {
                Request r = {request_id, 1, "RECONCILE-KEYS", "OK", ts};
                resp.push_back(r);
            }
            else {
                if (fd != server_fd) {
                    resp = send_and_recv(fd, req, wait_resp);
                }
                else {
                    Request r = {request_id, 1, "RECONCILE-KEYS", "OK", ts};
                    resp.push_back(r);
                }
            }
        }
    }
    else {
        send_message_to_hash(req, ip, port, hash);
    }

    return resp;
}

void Server::reconcile_keys_succ() {
    IpPort i_p = get_successor_ip_port();

    if (i_p.port != -1) {
        std::string curr_time = get_current_time();
        std::string request_id = curr_time + generate(5);
        
        Request r = {request_id, 0, "RECONCILE-KEYS", std::to_string(my_hash), std::stoull(curr_time)};
        reconcile_keys_rpc(r, i_p.ip, i_p.port);
    }
    else {
        std::cout << "Successors empty" << std::endl;
    }
}

// RECONCILE KEYS FROM SUCCESSOR (ENDS HERE)


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
        finger[(my_hash + p) % KEY_SPACE_SIZE] = my_hash;
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

        if (dist(key, val) > dist(key, hsh)) {
            finger[key] = hsh;
            updated = true;
        }
    }

    if (updated) {
        if (hash_to_socket_map.find(hsh) == hash_to_socket_map.end()) {
            IpPort i_p = get_ip_port(ip_port);
            int fd = add_connection(i_p.ip, i_p.port, hash_to_socket_map);
            add_fd_to_epoll(fd, EPOLLIN | EPOLLET | EPOLLOUT);
        }
    }
}

void Server::update_finger_table(std::string ip_port, unsigned long diff) {
    unsigned long hsh = get_hash(ip_port);
    bool updated = false;

    unsigned long key = (my_hash + diff) % KEY_SPACE_SIZE;
    unsigned long val = finger[key];

    if (dist(key, val) > dist(key, hsh)) {
        finger[key] = hsh;
        updated = true;
    }

    if (updated) {
        if (hash_to_socket_map.find(hsh) == hash_to_socket_map.end()) {
            IpPort i_p = get_ip_port(ip_port);
            int fd = add_connection(i_p.ip, i_p.port, hash_to_socket_map);
            add_fd_to_epoll(fd, EPOLLIN | EPOLLET | EPOLLOUT);
        }
    }
}

void Server::run_epoll() {
    struct sockaddr_in new_addr;
    int addrlen = sizeof(struct sockaddr_in);

    long stabilization_timeout = 0;
    long finger_timeout = 0;
    long reconcile_timeout = 0;

    while (1) {

        if (std::time(0) > stabilization_timeout) {
            update_successor_list();
            update_predecessor();
            update_predecessor_succ();
            update_successor_pred();

            std::cout << "PREDECESSOR : " << predecessor << std::endl;
            std::cout << "SUCCESSORS : " << std::endl;

            for (HashPair succ : successors) {
                std::cout << succ.inp << std::endl;
            }

            std::cout << std::endl;
            stabilization_timeout = std::time(0) + get_random(1, 2);
        }

        if (std::time(0) > finger_timeout) {
            update_chord_fingers();
            update_own_finger();

            std::cout << "Fingers" << std::endl;
            for (auto kv : finger) {
                std::cout << std::to_string(kv.first) << ":" << std::to_string(kv.second) << std::endl;
            }

            std::cout << std::endl;
            finger_timeout = std::time(0) + get_random(1, 10);
        }

        if (std::time(0) > reconcile_timeout) {
            reconcile_keys_succ();
            reconcile_timeout = std::time(0) + get_random(1, 20);
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
                std::vector<std::string> msgs;
                recv_from_socket(fd, msgs);

                for (std::string msg : msgs) {
                    handle_request(msg, fd);
                }
            }
        }
    }
}

unsigned long Server::get_next_server_hash_to_fwd(std::string key) {
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

    return server_hash;
}

int Server::get_next_server_to_fwd(std::string key) {
    unsigned long server_hash = get_next_server_hash_to_fwd(key);

    if (hash_to_socket_map.find(server_hash) != hash_to_socket_map.end()) {
        return hash_to_socket_map[server_hash];
    }

    return -1;
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

    return -1;
}

int Server::get_next_server_to_fwd_reverse(unsigned long hash) {
    auto it = finger.upper_bound(hash);

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

    return -1;
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

        if (type == 1) {
            if (request_id_to_fd.find(request_id) != request_id_to_fd.end()) {
                int fd = request_id_to_fd[request_id];
                std::string ser_req = serialize_request(req);
                send_message(fd, ser_req);
            }
            else {
                if (command == "GET-SUCCESSOR") {
                    if (req.data != "NULL" && get_hash(req.data) != my_hash) {
                        insert_successor(req.data);
                    }
                }

                else if (command == "GET-PREDECESSOR") {
                    if (req.data != "NULL" && get_hash(req.data) != my_hash) predecessor = req.data;
                }

                else if (command == "FINGER") {
                    if (req.data != "NULL") {
                        std::vector<std::string> parts = split(req.data, "-");
                        unsigned long p = std::stoul(parts[0]);
                        std::string ip_port = parts[1];

                        if (my_hash != get_hash(ip_port)) update_finger_table(ip_port, p);
                    }
                }

                else if (command == "RECONCILE-KEYS") {
                    std::cout << msg << std::endl;
                    put_kv(req);
                }
            }
        }
        else {
            request_id_to_fd[req.request_id] = fd;
            std::vector<Request> resp;

            if (command == "PUT") {
                resp = put_kv_rpc(req, public_ip, public_port);
            }

            else if (command == "GET") {
                resp = get_v_rpc(req, public_ip, public_port);
            }

            else if (command == "UPDATE-FINGER") {
                resp = set_finger_rpc(req, public_ip, public_port);
            }

            else if (command == "GET-SUCCESSOR") {
                resp = get_successors_rpc(req, public_ip, public_port);
            }

            else if (command == "GET-PREDECESSOR") {
                resp = get_predecessor_rpc(req, public_ip, public_port);
            }

            else if (command == "FINGER") {
                resp = get_finger_rpc(req, public_ip, public_port);
            }

            else if (command == "SET-PREDECESSOR") {
                resp = set_predecessor_rpc(req, public_ip, public_port);
            }

            else if (command == "SET-SUCCESSOR") {
                resp = set_successor_rpc(req, public_ip, public_port);
            }

            else if (command == "RECONCILE-KEYS") {
                resp = reconcile_keys_rpc(req, public_ip, public_port);
            }

            else if (command == "UPDATE-FINGER-PREDECESSOR") {
                resp = set_finger_pred_rpc(req, public_ip, public_port);
            }

            std::string res = "";

            for (Request r : resp) {
                std::string ser_req = serialize_request(r);
                res += ser_req;
            }

            send_message(fd, res);
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

    std::cout << std::to_string(server.my_hash) << std::endl;

    if (argc > 3) {
        // connect with one of random servers in the circle
        std::string p(argv[3], argv[3] + strlen(argv[3]));
        server.predecessor = p;

        HashPair hp = {server.my_hash, p};
        server.successors.insert(hp);
    }

    server.run_epoll();
}