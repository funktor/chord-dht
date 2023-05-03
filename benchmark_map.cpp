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
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/vector.hpp>

struct pair {
    std::string k;
    std::string v;
};

// Generate random string of max_length
std::string generate(int max_length){
    std::string possible_characters = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    std::random_device rd;
    std::mt19937 engine(rd());
    std::uniform_int_distribution<> dist(0, possible_characters.size()-1);
    std::string ret = "";

    for(int i = 0; i < max_length; i++){
        int random_index = dist(engine); //get index between 0 and possible_characters.size()-1
        ret += possible_characters[random_index];
    }
    return ret;
}

class HashMap {
    public:
    std::unordered_map<std::string, std::string> map;
    std::mutex m;

    void insert(std::string key, std::string val);
    std::string get(std::string key);
};

void HashMap::insert(std::string key, std::string val) {
    {
        std::lock_guard<std::mutex> lock (m);
        map[key] = val;
    }
}

std::string HashMap::get(std::string key) {
    {
        std::lock_guard<std::mutex> lock (m);
        if (map.find(key) != map.end()) {
            return map[key];
        }
        return "NOT_FOUND";
    }
}

int main(int argc, char *argv[]) {
    std::vector<std::thread> my_threads;
    std::vector<pair> pairs;

    int n = atoi(argv[1]);

    for (int i = 0; i < n; i++) {
        std::string k = generate(10);
        std::string v = generate(10);
        struct pair p = {k, v};
        pairs.push_back(p);
    } 

    HashMap hm;
    auto start = std::chrono::high_resolution_clock::now();
    for (auto p : pairs) {
        hm.insert(p.k, p.v);
    }
    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    std::cout << duration.count() << std::endl;
}
