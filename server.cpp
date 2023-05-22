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

// Split a string by delimiter. Modifies original string
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

// Split a string by delimiter. Does not modify original string
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

// Get a hash value
unsigned long get_hash(std::string key) {
    const std::hash<std::string> hasher;
    const auto hashResult = hasher(key);
    return hashResult % KEY_SPACE_SIZE;
}

// Get current time as a string
std::string get_current_time() {
    std::time_t t = std::time(0);
    return std::to_string(t);
}

// Get random integer b/w a and b
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

// Get distance b/w 2 points on the chord circle of size KEY_SPACE_SIZE
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

// Custom comparator for hashes on the chord ring
struct IpPortCmp {
    bool operator() (HashPair a, HashPair b) const {
        unsigned long h1 = get_hash(a.inp);
        unsigned long h2 = get_hash(b.inp);

        unsigned long d1 = dist(a.ref_hash, h1);
        unsigned long d2 = dist(b.ref_hash, h2);
        
        return d1 < d2;
    }
};

// Connect to ip and port using sockets
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

// Send message to socket
int send_to_socket(int fd, std::string msg) {
    const char *msg_chr = msg.c_str();
    int res = send(fd, msg_chr, strlen(msg_chr), MSG_NOSIGNAL);
    if (errno == EPIPE) res = -1;

    return res;
}

// Receive data from socket and store them in msgs
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
        else if (ret_data == 0) {
            break;
        }
        else {
            return -1;
        }
    }

    return 1;
}

// Receive data from socket using select (for timeout behaviour)
int recv_select(int fd, std::vector<std::string> &msgs, std::string sep="<EOM>", long timeout=10000) {
    fd_set read_fd_set;
    struct timeval tv = {0, timeout};

    while (1) {
        FD_ZERO(&read_fd_set);
        FD_SET(fd, &read_fd_set);
        
        int ret_val = select(FD_SETSIZE, &read_fd_set, NULL, NULL, &tv);

        if (ret_val != -1) {
            if (FD_ISSET(fd, &read_fd_set)) {
                recv_from_socket(fd, msgs, sep);
                break;
            }
        }
    }
    
    return 1;
}

// Deserialize string into Request object
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

// Extract IP and Port from string
IpPort get_ip_port(std::string ip_port_str) {
    std::vector<std::string> addr = split(ip_port_str, ":");
    IpPort out = {addr[0], std::stoi(addr[1])};
    return out;
}

// Send message as string to socket
int send_message(int fd, std::string req_str) {
    return send_to_socket(fd, req_str);
}

// Add a new connection
int add_connection(std::string ip, int port, std::unordered_map<unsigned long, int> &m) {
    int fd = connect(ip, port);
    unsigned long hash = get_hash(ip + ":" + std::to_string(port));
    m[hash] = fd;
    return fd;
}

// Insert a key value pair into hash table
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

// Serialize Request object into string
std::string serialize_request(Request req) {
    return req.request_id + " " + std::to_string(req.type) + " " + req.command + " " + req.data + " " + std::to_string(req.ts) + "<EOM>";
}

// Send message as Request object to socket
int send_message(int fd, Request req) {
    std::string ser = serialize_request(req);
    return send_to_socket(fd, ser);
}

// Check if hash values a, b, and c are in clockwise order in the chord ring
bool is_cyclic(unsigned long a, unsigned long b, unsigned long c) {
    bool x = (a < b) && (b <= c);
    bool y = (b <= c) && (c < a);
    bool z = (c < a) && (a < b);

    return x || y || z;
}

class Server {
    public:
    int server_fd = -1;
    int epoll_fd = -1;
    std::string public_ip;
    int public_port;
    unsigned long my_hash;
    std::string public_ip_port_str;
    struct epoll_event ev, events[MAX_EVENTS];
    std::unordered_map<unsigned long, int> hash_to_socket_map;
    std::unordered_map<std::string, HashValue> hash_table;
    std::unordered_map<std::string, int> request_id_to_fd;
    std::map<unsigned long, unsigned long> finger;
    std::set<HashPair, IpPortCmp> successors;
    std::unordered_set<unsigned long> successor_hashes;
    std::string predecessor="";

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
    IpPort get_successor_ip_port(int order);
    int create_socket_fd(std::string ip_port);

    int get_predecessor(Request req, std::vector<Request> &resp);
    int get_predecessor_rpc(Request req, std::string ip, int port, std::vector<Request> &resp);
    
    int get_successors(Request req, std::vector<Request> &resp);
    int get_successors_rpc(Request req, std::string ip, int port, std::vector<Request> &resp);

    int set_predecessor(Request req, std::vector<Request> &resp);
    int set_predecessor_rpc(Request req, std::string ip, int port, std::vector<Request> &resp);
    
    int set_successor(Request req, std::vector<Request> &resp);
    int set_successor_rpc(Request req, std::string ip, int port, std::vector<Request> &resp);

    int put_kv(Request req, std::vector<Request> &resp);
    int put_kv_rpc(Request req, std::string ip, int port, std::vector<Request> &resp);

    int get_v(Request req, std::vector<Request> &resp);
    int get_v_rpc(Request req, std::string ip, int port, std::vector<Request> &resp);

    int get_finger(Request req, std::vector<Request> &resp);
    int get_finger_rpc(Request req, std::string ip, int port, std::vector<Request> &resp);

    int set_finger(Request req, std::vector<Request> &resp);
    int set_finger_rpc(Request req, std::string ip, int port, std::vector<Request> &resp);

    int set_finger_pred(Request req, std::vector<Request> &resp);
    int set_finger_pred_rpc(Request req, std::string ip, int port, std::vector<Request> &resp);

    int reconcile_keys(Request req, std::vector<Request> &resp);
    int reconcile_keys_rpc(Request req, std::string ip, int port, std::vector<Request> &resp);

    void set_self_predecessor();
    void set_successor_of_pred();
    void set_successor_list();
    void set_predecessor_of_succ();
    void update_self_finger_table();
    void update_finger_table_others();
    void reconcile_keys_succ();
    void insert_successor(std::string ip_port);
    void insert_predecessor(std::string ip_port);
    int send_message_to_ip_port(Request req, std::string ip_port);
    int send_message_to_next(Request req, std::string ip_port);
    int send_message_to_next(Request req, unsigned long hash, bool reverse);
};

// Create a socket if not exists and add it to epoll
int Server::create_socket_fd(std::string ip_port) {
    unsigned long hash = get_hash(ip_port);
    int fd;

    if (hash_to_socket_map.find(hash) == hash_to_socket_map.end()) {
        IpPort i_p = get_ip_port(ip_port);
        fd = add_connection(i_p.ip, i_p.port, hash_to_socket_map);
        add_fd_to_epoll(fd, EPOLLIN | EPOLLERR | EPOLLOUT);
    }
    else {
        fd = hash_to_socket_map[hash];
    }
    
    return fd;
}

// Add a new successor
void Server::insert_successor(std::string ip_ports) {
    std::vector<std::string> ip_ports_vector = split(ip_ports, ",");

    for (std::string ip_port : ip_ports_vector) {
        if (get_hash(ip_port) != my_hash) {
            HashPair hp = {my_hash, ip_port};

            // Maximum of 32 successors can be added.
            if (successors.size() < 32) {
                successors.insert(hp);
                successor_hashes.insert(get_hash(ip_port));
            }
            else {
                // If number of successors is greater than 32, then remove the 
                // last one in the chord ring
                auto it = successors.rbegin();
                unsigned long end_hash = get_hash((*it).inp);
                unsigned long curr_hash = get_hash(ip_port);

                if (dist(my_hash, curr_hash) < dist(my_hash, end_hash)) {
                    successors.erase(*it);
                    successor_hashes.erase(end_hash);

                    successors.insert(hp);
                    successor_hashes.insert(curr_hash);
                }
            }
        }
    }
}

// Add a new predecessor
void Server::insert_predecessor(std::string ip_port) {
    if (predecessor == "") predecessor = ip_port;
    else {
        unsigned long pred_hash = get_hash(predecessor);
        unsigned long curr_hash = get_hash(ip_port);

        if (dist(curr_hash, my_hash) < dist(pred_hash, my_hash)) predecessor = ip_port;
    }
}

// Get ip and port of i-th successor from list
IpPort Server::get_successor_ip_port(int order=1) {
    IpPort out = {"", -1};

    int c = 1;

    if (successors.size() > 0) {
        auto it = successors.begin();
        while (c < order && it != successors.end()) {
            it++;
            c++;
        }
        if (it != successors.end()) {
            std::string ip_port = (*it).inp;
            IpPort i_p = get_ip_port(ip_port);
            return i_p;
        }
    }

    return out;
}

// Send message to socket identified by ip_port string
int Server::send_message_to_ip_port(Request req, std::string ip_port) {
    int succ_order = 1;
    while (1) {
        int fd = create_socket_fd(ip_port);
        int res = send_message(fd, req);

        if (res != -1) return 1;
        else {
            IpPort succ = get_successor_ip_port(succ_order);

            if (succ.port == -1) {
                std::cout << "No successor" << std::endl;
                return -1;
            }
            else {
                ip_port = succ.ip + ":" + std::to_string(succ.port);
                succ_order++;
            }
        }
    }

    return -1;
}

// Send message to 'farthest' socket in the finger table
int Server::send_message_to_next(Request req, std::string ip_port) {
    int succ_order = 1;

    int fd = get_next_server_to_fwd(ip_port);
    if (fd == -1 || fd == server_fd) return -1;

    while (1) {
        int res = send_message(fd, req);

        if (res != -1) return 1;
        else {
            IpPort succ = get_successor_ip_port(succ_order);

            if (succ.port == -1) {
                std::cout << "No successor" << std::endl;
                return -1;
            }
            else {
                std::string ip_port = succ.ip + ":" + std::to_string(succ.port);
                fd = create_socket_fd(ip_port);
                succ_order++;
            }
        }
    }

    return -1;
}

// Send message to 'farthest' socket in the finger table
int Server::send_message_to_next(Request req, unsigned long hash, bool reverse=false) {
    int succ_order = 1;

    int fd;
    if (reverse) fd = get_next_server_to_fwd_reverse(hash);
    else fd = get_next_server_to_fwd(hash);

    if (fd == -1 || fd == server_fd) return -1;

    while (1) {
        int res = send_message(fd, req);

        if (res != -1) return 1;
        else {
            IpPort succ = get_successor_ip_port(succ_order);

            if (succ.port == -1) {
                std::cout << "No successor" << std::endl;
                return -1;
            }
            else {
                std::string ip_port = succ.ip + ":" + std::to_string(succ.port);
                fd = create_socket_fd(ip_port);
                succ_order++;
            }
        }
    }

    return -1;
}

// SET PREDECESSOR (STARTS HERE)

int Server::get_predecessor(Request req, std::vector<Request> &resp) {
    std::string request_id = req.request_id;
    unsigned long long ts = req.ts;

    std::string pred;

    if (predecessor != "") pred = predecessor;
    else pred = public_ip_port_str;

    Request r = {request_id, 1, "GET-PREDECESSOR", pred, ts};
    resp.push_back(r);

    return 1;
}

int Server::get_predecessor_rpc(Request req, std::string ip, int port, std::vector<Request> &resp) {
    std::string ip_str = ip + ":" + std::to_string(port);
    int res = -1;
    unsigned long hash = get_hash(ip_str);

    if (hash == my_hash) {
        res = get_predecessor(req, resp);
    }
    else {
        res = send_message_to_ip_port(req, ip_str);
    }

    if (res == -1) {
        Request r = {req.request_id, 1, "GET-PREDECESSOR", "NULL", req.ts};
        resp.push_back(r);
    }

    return res;
}

void Server::set_self_predecessor() {
    std::vector<Request> resp;
    IpPort i_p = get_successor_ip_port();

    if (i_p.port != -1) {
        std::string curr_time = get_current_time();
        std::string request_id = curr_time + generate(5);
    
        Request r = {request_id, 0, "GET-PREDECESSOR", public_ip_port_str, std::stoull(curr_time)};
        get_predecessor_rpc(r, i_p.ip, i_p.port, resp);
    }
    else {
        std::cout << "Successors empty" << std::endl;
    }
}

// SET PREDECESSOR (ENDS HERE)

// SET SUCCESSOR LIST (STARTS HERE)

int Server::get_successors(Request req, std::vector<Request> &resp) {
    std::string request_id = req.request_id;
    unsigned long long ts = req.ts;

    std::string output = public_ip_port_str;

    for (auto succ : successors) {
        output += ",";
        output += succ.inp;
    }

    Request r = {request_id, 1, "GET-SUCCESSOR", output, ts};
    resp.push_back(r);

    return 1;
}

int Server::get_successors_rpc(Request req, std::string ip, int port, std::vector<Request> &resp) {
    std::string ip_str = ip + ":" + std::to_string(port);
    int res = -1;
    unsigned long hash = get_hash(ip_str);

    if (hash == my_hash) {
        unsigned long pred_hash = get_hash(predecessor);

        if ((predecessor == "") || is_cyclic(pred_hash, hash, my_hash) || pred_hash == hash) {
            res = get_successors(req, resp);
        }
        else {
            res = send_message_to_next(req, ip_str);
        }
    }
    else {
        res = send_message_to_ip_port(req, ip_str);
    }

    if (res == -1) {
        Request r = {req.request_id, 1, "GET-SUCCESSOR", "NULL", req.ts};
        resp.push_back(r);
    }

    return res;
}

void Server::set_successor_list() {
    std::vector<Request> resp;
    IpPort i_p = get_successor_ip_port();

    if (i_p.port != -1) {
        std::string curr_time = get_current_time();
        std::string request_id = curr_time + generate(5);
    
        Request r = {request_id, 0, "GET-SUCCESSOR", public_ip_port_str, std::stoull(curr_time)};
        get_successors_rpc(r, i_p.ip, i_p.port, resp);
    }
    else {
        std::cout << "Successors empty" << std::endl;
    }
}

// SET SUCCESSOR LIST (ENDS HERE)

// SET PREDECESSOR OF SUCCESSOR (STARTS HERE)

int Server::set_predecessor(Request req, std::vector<Request> &resp) {
    std::string data = req.data;
    std::string request_id = req.request_id;
    unsigned long long ts = req.ts;

    std::string ip_port = data;
    predecessor = ip_port;

    if (successors.size() == 0) insert_successor(ip_port);

    Request r = {request_id, 1, "SET-PREDECESSOR", "OK", ts};
    resp.push_back(r);

    return 1;
}

int Server::set_predecessor_rpc(Request req, std::string ip, int port, std::vector<Request> &resp) {
    std::string ip_str = ip + ":" + std::to_string(port);
    int res = -1;
    unsigned long hash = get_hash(ip_str);

    if (hash == my_hash) {
        res = set_predecessor(req, resp);
    }
    else {
        res = send_message_to_ip_port(req, ip_str);
    }

    if (res == -1) {
        Request r = {req.request_id, 1, "SET-PREDECESSOR", "KO", req.ts};
        resp.push_back(r);
    }

    return res;
}

void Server::set_predecessor_of_succ() {
    std::vector<Request> resp;
    IpPort i_p = get_successor_ip_port();

    if (i_p.port != -1) {
        std::string curr_time = get_current_time();
        std::string request_id = curr_time + generate(5);
    
        Request r = {request_id, 0, "SET-PREDECESSOR", public_ip_port_str, std::stoull(curr_time)};
        set_predecessor_rpc(r, i_p.ip, i_p.port, resp);
    }
    else {
        std::cout << "Successors empty" << std::endl;
    }
}

// SET PREDECESSOR OF SUCCESSOR (ENDS HERE)

// SET SUCCESSOR OF PREDECESSOR (STARTS HERE)

int Server::set_successor(Request req, std::vector<Request> &resp) {
    std::string data = req.data;
    std::string request_id = req.request_id;
    unsigned long long ts = req.ts;

    std::string ip_port = data;
    insert_successor(ip_port);

    Request r = {request_id, 1, "SET-SUCCESSOR", "OK", ts};
    resp.push_back(r);

    return 1;
}

int Server::set_successor_rpc(Request req, std::string ip, int port, std::vector<Request> &resp) {
    std::string ip_str = ip + ":" + std::to_string(port);
    unsigned long hash = get_hash(ip_str);
    int res = -1;

    if (hash == my_hash) {
        res = set_successor(req, resp);
    }
    else {
        res = send_message_to_ip_port(req, ip_str);
    }

    if (res == -1) {
        Request r = {req.request_id, 1, "SET-SUCCESSOR", "KO", req.ts};
        resp.push_back(r);
    }

    return res;
}

void Server::set_successor_of_pred() {
    std::vector<Request> resp;

    if (predecessor != "") {
        IpPort i_p = get_ip_port(predecessor);
        std::string curr_time = get_current_time();
        std::string request_id = curr_time + generate(5);
    
        Request r = {request_id, 0, "SET-SUCCESSOR", public_ip_port_str, std::stoull(curr_time)};
        set_successor_rpc(r, i_p.ip, i_p.port, resp);
    }
    else {
        std::cout << "Predecessor empty" << std::endl;
    }
}

// SET SUCCESSOR OF PREDECESSOR (ENDS HERE)

// PUT KEY-VALUE PAIR (STARTS HERE)

int Server::put_kv(Request req, std::vector<Request> &resp) {
    std::string data = req.data;
    std::string request_id = req.request_id;
    unsigned long long ts = req.ts;

    insert_kv_pair(data, req.ts, hash_table);

    Request r = {request_id, 1, "PUT", "UPDATED", ts};
    resp.push_back(r);

    return 1;
}

int Server::put_kv_rpc(Request req, std::string ip, int port, std::vector<Request> &resp) {
    std::string data = req.data;
    std::string request_id = req.request_id;
    unsigned long long ts = req.ts;

    std::vector<std::string> key_val = split(data, ":");

    std::string key = key_val[0];
    std::string val = key_val[1];

    unsigned long key_hash = get_hash(key);
    unsigned long pred_hash = get_hash(predecessor);

    int res = -1;

    if ((predecessor == "") || is_cyclic(pred_hash, key_hash, my_hash) || pred_hash == key_hash) {
        res = put_kv(req, resp);
    }

    else {
        res = send_message_to_next(req, key);
    }

    if (res == -1) {
        Request r = {req.request_id, 1, "PUT", "ERROR", req.ts};
        resp.push_back(r);
    }

    return res;
}

// PUT KEY-VALUE PAIR (ENDS HERE)

// GET VALUE CORRESPONDING TO KEY (STARTS HERE)

int Server::get_v(Request req, std::vector<Request> &resp) {
    std::string data = req.data;
    std::string request_id = req.request_id;
    unsigned long long ts = req.ts;

    std::string key = data;
    std::string val = "<EMPTY>";

    if (hash_table.find(key) != hash_table.end()) {
        HashValue hv = hash_table[key];
        val = hv.val;
    }

    Request r = {request_id, 1, "GET", val, ts};
    resp.push_back(r);

    return 1;
}

int Server::get_v_rpc(Request req, std::string ip, int port, std::vector<Request> &resp) {
    std::string data = req.data;
    std::string request_id = req.request_id;
    unsigned long long ts = req.ts;

    std::string key = data;

    unsigned long key_hash = get_hash(key);
    unsigned long pred_hash = get_hash(predecessor);

    int res = -1;

    if ((predecessor == "") || is_cyclic(pred_hash, key_hash, my_hash) || pred_hash == key_hash) {
        res = get_v(req, resp);
    }

    else {
        res = send_message_to_next(req, key);
    }

    if (res == -1) {
        Request r = {req.request_id, 1, "GET", "<EMPTY>", req.ts};
        resp.push_back(r);
    }

    return res;
}

// GET VALUE CORRESPONDING TO KEY (ENDS HERE)

// GET FINGER TABLE ENTRIES (STARTS HERE)

int Server::get_finger(Request req, std::vector<Request> &resp) {
    std::string data = req.data;
    std::string request_id = req.request_id;
    unsigned long long ts = req.ts;

    std::vector<std::string> parts = split(data, "-");
    unsigned long j = std::stoul(parts[0]);

    Request r = {request_id, 1, "FINGER", std::to_string(j) + "-" + public_ip + ":" + std::to_string(public_port), ts};

    resp.push_back(r);

    return 1;
}

int Server::get_finger_rpc(Request req, std::string ip, int port, std::vector<Request> &resp) {
    std::string ip_str = ip + ":" + std::to_string(port);

    int res = -1;
    unsigned long hash = get_hash(ip_str);

    if (hash == my_hash) {
        std::string data = req.data;
        std::string request_id = req.request_id;
        unsigned long long ts = req.ts;

        std::vector<std::string> parts = split(data, "-");
        unsigned long j = std::stoul(parts[0]);

        unsigned long node_hash = std::stoul(parts[1]);
        unsigned long pred_hash = get_hash(predecessor);
        
        if ((predecessor == "") || is_cyclic(pred_hash, node_hash, my_hash) || pred_hash == node_hash) {
            res = get_finger(req, resp);
        }
        else {
            res = send_message_to_next(req, node_hash);
        }
    }
    else {
        res = send_message_to_ip_port(req, ip_str);
    }

    if (res == -1) {
        Request r = {req.request_id, 1, "FINGER", "NULL", req.ts};
        resp.push_back(r);
    }

    return res;
}

void Server::update_self_finger_table() {
    std::vector<Request> resp;
    IpPort i_p = get_successor_ip_port();

    if (i_p.port != -1) {
        unsigned long p = 1;

        for (int i = 0; i < 32; i++) {
            unsigned long key = (my_hash + p) % KEY_SPACE_SIZE;

            std::string curr_time = get_current_time();
            std::string request_id = curr_time + generate(5);
        
            std::string data = std::to_string(p) + "-" + std::to_string(key);

            Request r = {request_id, 0, "FINGER", data, std::stoull(curr_time)};
            get_finger_rpc(r, i_p.ip, i_p.port, resp);

            p *= 2;
        }
    }
    else {
        std::cout << "Successors empty" << std::endl;
    }
}

// GET FINGER TABLE ENTRIES (ENDS HERE)

// UPDATE FINGER TABLES OF CHORD (STARTS HERE)

int Server::set_finger(Request req, std::vector<Request> &resp) {
    std::string data = req.data;
    std::string request_id = req.request_id;
    unsigned long long ts = req.ts;

    std::vector<std::string> parts = split(data, "-");

    unsigned long j = std::stoul(parts[0]);
    std::string ip_port = parts[1];

    update_finger_table(ip_port, j);

    Request r = {request_id, 1, "UPDATE-FINGER", "OK", ts};
    resp.push_back(r);

    return 1;
}

int Server::set_finger_rpc(Request req, std::string ip, int port, std::vector<Request> &resp) {
    std::string ip_str = ip + ":" + std::to_string(port);

    int res = -1;
    unsigned long hash = get_hash(ip_str);

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
            res = set_finger_pred_rpc(req, ip, port, resp);
        }

        else {
            unsigned long new_hash = dist(j, node_hash);
            res = send_message_to_next(req, new_hash, true);
        }
    }
    else {
        res = send_message_to_ip_port(req, ip_str);
    }

    if (res == -1) {
        Request r = {req.request_id, 1, "UPDATE-FINGER", "KO", req.ts};
        resp.push_back(r);
    }

    return res;
}

void Server::update_finger_table_others() {
    std::vector<Request> resp;
    IpPort i_p = get_successor_ip_port();

    if (i_p.port != -1) {
        unsigned long p = 1;

        for (int i = 0; i < 32; i++) {
            std::string data = std::to_string(p) + "-" + public_ip_port_str;

            std::string curr_time = get_current_time();
            std::string request_id = curr_time + generate(5);
        
            Request r = {request_id, 0, "UPDATE-FINGER", data, std::stoull(curr_time)};
            set_finger_rpc(r, i_p.ip, i_p.port, resp);

            p *= 2;
        }
    }
    else {
        std::cout << "Successors empty" << std::endl;
    }
}

// UPDATE FINGER TABLES OF CHORD (ENDS HERE)

// UPDATE FINGER TABLE OF PREDECESSOR (STARTS HERE)

int Server::set_finger_pred(Request req, std::vector<Request> &resp) {
    std::string data = req.data;
    std::string request_id = req.request_id;
    unsigned long long ts = req.ts;

    Request r = {request_id, 1, "UPDATE-FINGER-PREDECESSOR", "OK", ts};
    resp.push_back(r);

    return 1;
}

int Server::set_finger_pred_rpc(Request req, std::string ip, int port, std::vector<Request> &resp) {
    std::string ip_str = ip + ":" + std::to_string(port);

    int res = -1;
    unsigned long hash = get_hash(ip_str);

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

            IpPort succ = get_successor_ip_port();
            unsigned long succ_hash = get_hash(succ.ip + ":" + std::to_string(succ.port));

            if (predecessor != "" && pred_hash != succ_hash) {
                IpPort i_p = get_ip_port(predecessor);
                res = set_finger_pred_rpc(req, i_p.ip, i_p.port, resp);
            }
            else {
                res = set_finger_pred(req, resp);
            }
        }
        else {
            res = set_finger_pred(req, resp);
        }
    }
    else {
        res = send_message_to_ip_port(req, ip_str);
    }

    if (res == -1) {
        Request r = {req.request_id, 1, "UPDATE-FINGER-PREDECESSOR", "KO", req.ts};
        resp.push_back(r);
    }

    return res;
}

// UPDATE FINGER TABLE OF PREDECESSOR (ENDS HERE)


// RECONCILE KEYS FROM SUCCESSOR (STARTS HERE)

int Server::reconcile_keys(Request req, std::vector<Request> &resp) {
    std::string data = req.data;
    std::string request_id = req.request_id;
    unsigned long long ts = req.ts;

    unsigned long hash = std::stoul(data);

    std::vector<std::string> keys_to_delete;
    std::string output = "";

    for (auto kv : hash_table) {
        std::string k = kv.first;
        HashValue hv = kv.second;
        std::string v = hv.val;
        unsigned long long ts = hv.timestamp;

        unsigned long key_hash = get_hash(k);

        if (dist(key_hash, hash) < dist(key_hash, my_hash)) {
            std::string request_id = get_current_time() + generate(5);

            if (output != "") output += ",";
            output += k + ":" + v + ":" + std::to_string(ts);

            keys_to_delete.push_back(k);
        }
    }

    for (auto k : keys_to_delete) {
        hash_table.erase(k);
    }

    if (output != "") {
        Request r = {request_id, 1, "RECONCILE-KEYS", output, ts};
        resp.push_back(r);
    }

    return 1;
}

int Server::reconcile_keys_rpc(Request req, std::string ip, int port, std::vector<Request> &resp) {
    std::string ip_port = ip + ":" + std::to_string(port);
    unsigned long hash = get_hash(ip_port);

    int res = -1;

    if (hash == my_hash) {
        res = reconcile_keys(req, resp);
    }
    else {
        res = send_message_to_ip_port(req, ip_port);
    }

    if (res == -1) {
        Request r = {req.request_id, 1, "RECONCILE-KEYS", "NULL", req.ts};
        resp.push_back(r);
    }

    return res;
}

void Server::reconcile_keys_succ() {
    std::vector<Request> resp;
    IpPort i_p = get_successor_ip_port();

    if (i_p.port != -1) {
        std::string curr_time = get_current_time();
        std::string request_id = curr_time + generate(5);
        
        Request r = {request_id, 0, "RECONCILE-KEYS", std::to_string(my_hash), std::stoull(curr_time)};
        reconcile_keys_rpc(r, i_p.ip, i_p.port, resp);
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

    public_ip_port_str = public_ip + ":" + std::to_string(public_port);
    my_hash = get_hash(public_ip_port_str);

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
    std::vector<unsigned long> updated_vals;

    for (auto kv : finger) {
        unsigned long key = kv.first;
        unsigned long val = kv.second;

        if (dist(key, val) > dist(key, hsh)) {
            finger[key] = hsh;
            updated = true;
            updated_vals.push_back(val);
        }
    }

    if (updated) {
        for (unsigned long val : updated_vals) {
            if (hash_to_socket_map.find(val) != hash_to_socket_map.end()) {
                if (val != get_hash(predecessor) && successor_hashes.find(val) == successor_hashes.end()) {
                    int fd = hash_to_socket_map[val];
                    del_fd_from_epoll(fd);
                    hash_to_socket_map.erase(val);
                }
            }
        }
        
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
        if (hash_to_socket_map.find(val) != hash_to_socket_map.end()) {
            if (val != get_hash(predecessor) && successor_hashes.find(val) == successor_hashes.end()) {
                int fd = hash_to_socket_map[val];
                del_fd_from_epoll(fd);
                hash_to_socket_map.erase(val);
            }
        }

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
        try {
            if (std::time(0) > stabilization_timeout) {
                set_successor_list();
                set_self_predecessor();
                set_predecessor_of_succ();
                set_successor_of_pred();

                std::cout << "PREDECESSOR : " << predecessor << std::endl;
                std::cout << "SUCCESSORS : " << std::endl;

                for (HashPair succ : successors) {
                    std::cout << succ.inp << std::endl;
                }

                std::cout << std::endl;
                stabilization_timeout = std::time(0) + get_random(1, 2);
            }

            if (std::time(0) > finger_timeout) {
                update_finger_table_others();
                update_self_finger_table();

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

                else if (events[i].events & EPOLLIN) {
                    std::vector<std::string> msgs;
                    recv_from_socket(fd, msgs);

                    for (std::string msg : msgs) {
                        handle_request(msg, fd);
                    }
                }
            }
        }
        catch(...) {
            std::exception_ptr p = std::current_exception();
            std::clog <<(p ? p.__cxa_exception_type()->name() : "null") << std::endl;
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

    // std::cout << msg << std::endl;

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
                    if (req.data != "NULL" && get_hash(req.data) != my_hash) {
                        insert_predecessor(req.data);
                    }
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
                    if (req.data != "NULL") {
                        std::vector<std::string> kvs = split(req.data, ",");

                        for (std::string kv : kvs) {
                            std::cout << kv << std::endl;
                            std::vector<std::string> q = split(kv, ":");
                            insert_kv_pair(kv, std::stoull(q[2]), hash_table);
                        }
                    }
                }
            }
        }
        else {
            request_id_to_fd[req.request_id] = fd;
            std::vector<Request> resp;
            int res = -1;

            if (command == "PUT") {
                res = put_kv_rpc(req, public_ip, public_port, resp);
            }

            else if (command == "GET") {
                res = get_v_rpc(req, public_ip, public_port, resp);
            }

            else if (command == "UPDATE-FINGER") {
                res = set_finger_rpc(req, public_ip, public_port, resp);
            }

            else if (command == "GET-SUCCESSOR") {
                res = get_successors_rpc(req, public_ip, public_port, resp);
            }

            else if (command == "GET-PREDECESSOR") {
                res = get_predecessor_rpc(req, public_ip, public_port, resp);
            }

            else if (command == "FINGER") {
                res = get_finger_rpc(req, public_ip, public_port, resp);
            }

            else if (command == "SET-PREDECESSOR") {
                res = set_predecessor_rpc(req, public_ip, public_port, resp);
            }

            else if (command == "SET-SUCCESSOR") {
                res = set_successor_rpc(req, public_ip, public_port, resp);
            }

            else if (command == "RECONCILE-KEYS") {
                res = reconcile_keys_rpc(req, public_ip, public_port, resp);
            }

            else if (command == "UPDATE-FINGER-PREDECESSOR") {
                res = set_finger_pred_rpc(req, public_ip, public_port, resp);
            }

            if (res != -1) {
                std::string res = "";

                for (Request r : resp) {
                    std::string ser_req = serialize_request(r);
                    res += ser_req;
                }

                send_message(fd, res);
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

    std::cout << std::to_string(server.my_hash) << std::endl;

    if (argc > 3) {
        // connect with one of random servers in the circle
        std::string p(argv[3], argv[3] + strlen(argv[3]));

        // initialize random machine as successor
        server.insert_successor(p);
    }

    server.run_epoll();
}