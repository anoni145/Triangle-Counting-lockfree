// swapping ostringstream objects
#include <string>       // string
#include <iostream>     // cout
#include <sstream>      // stringstream
#include <fstream>
#include <math.h>
#include <thread>
#include <vector>
#include <string>

using namespace std;


void threadFunc(int numfrom , int numTo,int threadNo, int total_nodes){
    fstream log_file;
	log_file.open("fully_connected_10_5" +to_string(threadNo) + ".txt", ios::out);
    string buffer = ""; 
    for( int i = numfrom ;i < numTo ; i++){
        for(int j= i+1; j< total_nodes ; j++){
            buffer += to_string(i) + " " + to_string(j) + "\n";
            if(buffer.length() > 1 * (int)pow(10, 9)){
                log_file.write(buffer.c_str() , buffer.length());
                // log_file.write(buffer , buffer.length());
                buffer = "";
            }

        }
    }

    if(buffer.length() <= 1 * (int)pow(10, 9)){
        log_file.write(buffer.c_str() , buffer.length());
        // log_file.write(buffer , buffer.length());
        buffer = "";
    }

    log_file.close();
}

int main(int agrc, char** argv)
{
    
    int number_of_threads, power_cnt;
    number_of_threads = 1;
    power_cnt = 1;
    if(agrc > 1){
        number_of_threads = stoi(argv[2]);
        power_cnt = stoi(argv[1]);
    }
    
    vector<std::thread> thrds;
	thrds.reserve(number_of_threads);
    int total_nodes = power_cnt * (int)pow(10, 5);
    int chunk_size = (int) total_nodes / number_of_threads;

    for(int i = 0 ; i < number_of_threads ; i +=1){
        thrds.emplace_back(std::thread(threadFunc, i * chunk_size, (i+1)* chunk_size , i, total_nodes));
    }

	// loop again to join the threads
	for (int i = 0; i < thrds.size(); i++) {
		if (thrds[i].joinable())
			thrds[i].join();
	}


    return 0;
}