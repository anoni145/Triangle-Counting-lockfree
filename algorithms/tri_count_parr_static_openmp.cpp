#include <iostream>
#include <atomic>
#include <vector>
#include <fstream>
#include <bits/stdc++.h>
#include <map>
#include <unordered_map>
#include <atomic>
#include <thread>
#include <chrono>
#include <regex>
#include <omp.h>

using namespace std;


class Graph{
    long total_nodes ;
 

    vector<long> cummulativeIndex;
    vector<long> nodeIndex;
    atomic<long> no_of_triangles{
        0
    };

    


    public:

    Graph(long total_nodes){
        this->total_nodes = total_nodes;
    }

    ~Graph(){
        
    }

    vector<long> getCummulativeIndex(){
        return this->cummulativeIndex;
    }

    vector<long> getNodeIndex(){
        return this->nodeIndex;
    }

    long getNoOfTriangles()
    {
        return this->no_of_triangles;
    }

    long getTotalNodes(){
        return this->total_nodes;
    }

    

    void setCummulativeIndex(vector<long> cummulativeIndex){
        this->cummulativeIndex = cummulativeIndex;
    }

    void setNodeIndex(vector<long> nodeIndex){
        this->nodeIndex = nodeIndex;
    }


    

    //this method fetches the nearest neighbour from the 2 array
    vector<int> nearest_neighbours(long node)
    {
       long no_of_neighbours = cummulativeIndex[node + 1] - cummulativeIndex[node];
		// long *neighbors = new int(no_of_neighbours);
		vector<int> neighbours;
		neighbours.reserve(no_of_neighbours);

		long start_index  = cummulativeIndex[node];
		long end_index = cummulativeIndex[node + 1];

		long index = 0;
		for ( long i = start_index ; i< end_index ; i++){
				neighbours.push_back(nodeIndex[i]);
		}
        // sorts the neighbour based on thier index. This speed up removing elements that have already been visited.
		sort(neighbours.begin(), neighbours.end());

		return neighbours;
    }


    //checks whether 2 nodes are connected
    bool check_connected(long node1, long node2)
    {
        long node1_no_of_neighbours = cummulativeIndex[node1 + 1] - cummulativeIndex[node1];
        long node2_no_of_neighbours = cummulativeIndex[node2 + 1] - cummulativeIndex[node2];

        if(node1_no_of_neighbours > node2_no_of_neighbours)
        {
        //O(d)
                for (long i = cummulativeIndex[node2] ;  i < cummulativeIndex[node2 +1] ; i++)
                    if(nodeIndex[i] == node1)
                        return true;
                return false;
            
        }
        else
        {
            //O(d)
                for (long i = cummulativeIndex[node1] ;  i < cummulativeIndex[node1 +1]; i++)
                    if(nodeIndex[i] == node2)
                            return true;
                return false;
            
        }
    }


    // count the number of traingles in which the current node is assiciated after removing the already visited vertices
    int triangle_count(long curr_node , fstream *logfile, bool debug)
    {

        

        int triangle_cnt = 0;
		long remain_neigh_start_ind; //all the adjacent nodes for a given node
		// compute neighbors of node// neighbour_array -> []
		int num_of_neighbours = 0; 
		vector<int> neighbour_array = nearest_neighbours(curr_node);
		
	
		num_of_neighbours = neighbour_array.size();
        if(debug)
		    (*logfile) << "Thread id : " << this_thread::get_id() << " Node Id : " <<  curr_node  << " No of neighbours : " << num_of_neighbours << endl;
		

        if(num_of_neighbours < 2){
            return 0;
        }

        //we perform binary search here and (l) returns the index of least element greater than curr node 
		int  l = 0;
		int r = num_of_neighbours-1;
		int m = -1;
		while (l <= r) {
			m = l + (r - l) / 2;

		
			// If x greater, ignore left half
			if (neighbour_array[m] < curr_node){
				l = m + 1;
			}

			// If x is smaller, ignore right half
			else
				r = m - 1;
			
		}
		if(l > num_of_neighbours -1){
			if(neighbour_array[num_of_neighbours-1] < curr_node){
				return 0;
			}
		}
		
        //stores the index from which remaining elements starts from 
		int remaining_neighbour_cnt = num_of_neighbours - l;

		remain_neigh_start_ind = l;
        if(debug)
            (*logfile) << "Thread id : " << this_thread::get_id() << " Node Id : " <<  curr_node  << " Remaining neighbour count : " << remaining_neighbour_cnt << endl;
       

		if(remaining_neighbour_cnt < 2)
		{
			return 0;
		}

		
        
		// find remainig elements amongst the neighbouring nodes are connected
		for( int i = remain_neigh_start_ind ; i< neighbour_array.size() ;i++){
			for(int j=i+1; j< neighbour_array.size()  ;j++){

				long node1 = neighbour_array[i];
				long node2 = neighbour_array[j];
				if(check_connected(node1, node2)){
					triangle_cnt++;
				}

			}
		}
		
        if(debug){
  		    (*logfile) << "Thread id : " << this_thread::get_id() << " Node Id : " <<  curr_node  << " Triangle Cnt : " << triangle_cnt << endl;
            (*logfile) << endl; 
        }
        //this->no_of_triangles+= triangle_cnt;
        return triangle_cnt;
    }

};

void thread_funct(Graph *graph, fstream * logfile , bool debug, int thread_id, int no_of_threads){

    for (long node_id = thread_id; node_id < graph->getTotalNodes() -2; node_id += no_of_threads){
        if(debug)
            (*logfile) << "Thread id : " << this_thread::get_id() << " Node Id : " <<  node_id << endl;
        double startTime = chrono::duration_cast < chrono::milliseconds > (chrono::system_clock::now().time_since_epoch()).count();
        graph->triangle_count( node_id, logfile, debug);
        double endTime = chrono::duration_cast < chrono::milliseconds > (chrono::system_clock::now().time_since_epoch()).count();
        if(debug){
            (*logfile) << "Thread id : " << this_thread::get_id() << " Node Id : " <<  node_id << " Time taken : " << endTime - startTime << endl;
            (*logfile) << "Thread id : " << this_thread::get_id() << " Total Traingles : " <<  graph->getNoOfTriangles() << endl;
            (*logfile) << endl;
        }
    }

}

int main(int argc, char** argv){



    string logFileName = "../output/logfileParr.txt";
	string dataset = "../datasets/graph500-scale18.txt";
    string numberOfThreadsStr = "8";
	bool debug = true;
	if(argc > 1){
		logFileName = argv[1]  ;
		dataset = argv[2] ; 
        numberOfThreadsStr = argv[3];
		if(argc == 5){
			 if(argv[4] == "debug"){
            	debug = true;
       		 }
		}
	}
    
    fstream logfile;
    if(debug)
        logfile.open(logFileName,ios::out);
    ifstream input_file;
	input_file.open(dataset,ios::in);
	


    int   num_of_threads = stoi(numberOfThreadsStr);


	

	long total_nz_elements = 0;

    //for storin graph in CSR format
    //stores the cumulative sum of neighboring nodes starting from 0
	vector<long> cummulativeIndex;
    vector<long> nodeIndex;
	
	vector<pair<long,long>> neighbourCnts;

	//This stores the neighbour count and prev index
	unordered_map<long , set<long>> neighbourMap;

	
	

    long startInd;
	long neighbourInd;
    string line;
	//Creates a map of index and the set of neightbouring node index
	while(getline(input_file,line)){
		
		stringstream strStream;
	
		strStream << line; //convert the string s into stringstream

		string temp;
		int num;
		int index = 0;
		while(!strStream.eof()){
			strStream >> temp; //take words into temp_str one by one
			
			if(stringstream(temp) >> num){ //try to convert string to int
				if(index == 0)
					startInd = num;
				else
					neighbourInd = num;
			}
			
			index++;
		}
	
	
		
		
		//check if the this is the first fromInd element
		if(neighbourMap[startInd].size() == 0)
		{
			set<long> s1;
			s1.insert(neighbourInd);
			neighbourMap[startInd] = s1;
		}
		else{
			neighbourMap[startInd].insert(neighbourInd);
		}

        if(neighbourMap[neighbourInd].size() == 0)
		{
			set<long> s1;
			s1.insert(startInd);
			neighbourMap[neighbourInd] = s1;
		}
		else{
			neighbourMap[neighbourInd].insert(startInd);
		}

		total_nz_elements++;
	
	}

	//this stores a pair of neighboursize and prev index(will be used when we reorder the nodes based on size)
	for(auto x : neighbourMap){
		neighbourCnts.push_back(make_pair(x.second.size(),x.first));
	}

    //sorts the nodes based on the neighbour size()
	sort(neighbourCnts.begin(), neighbourCnts.end());


    
    
    long prev_index;
    total_nz_elements = 0;
	// 0 is the first element
    cummulativeIndex.push_back(total_nz_elements);

    unordered_map<long , long> prevIndexMap ;

    if(debug)
	    logfile <<"Main Thread : "<< " Index mapping" << endl;
	//maps prev index to new index
    for (long i = 0; i < neighbourCnts.size(); i++) {
        prev_index = neighbourCnts[i].second;
        prevIndexMap[prev_index] = i;
        if(debug)
		    logfile << prev_index << "   " << i << endl;
    }
    if(debug)
	    logfile << endl;
    long newIndex = -1;

    //reorders and updates based on the two arrays of csr
    for (long i = 0; i < neighbourCnts.size(); i++) {
        if(debug)
        {
            logfile << "Main Thread : " << "Node Id : " << i ;
            logfile << " Neighbours : " << endl;
        }
        prev_index = neighbourCnts[i].second;
		set<long> neighbourSet = neighbourMap[prev_index];
		total_nz_elements += neighbourCnts[i].first;//stores number of neighbours
		cummulativeIndex.push_back(total_nz_elements);
		set<long> ::iterator itr;


        


		for( itr = neighbourSet.begin() ; itr != neighbourSet.end() ; itr++){
			nodeIndex.push_back(prevIndexMap[*itr]);
            if(debug)
                logfile << prevIndexMap[*itr] << " ";
		}
        if(debug)
            logfile << endl;
    }
	

    

    long total_nodes = cummulativeIndex.size() - 1;


    vector<std::thread> thrds;
    thrds.reserve(num_of_threads);

    Graph *graph = new Graph(total_nodes);
    graph->setCummulativeIndex(cummulativeIndex);
    graph->setNodeIndex(nodeIndex);

	chrono::high_resolution_clock::time_point startT = chrono::high_resolution_clock::now(); 
    

    //for (int thread_id =0;thread_id < num_of_threads; thread_id++){
        
    //    thrds.push_back(std::thread(thread_funct, graph , &logfile , debug, thread_id , num_of_threads) );

    //}

    //for(int i= 0;i < thrds.size();i++)
    //{
    //    if(thrds[i].joinable())
    //        thrds[i].join();
    //}
    omp_set_dynamic( 0 );
    //omp_set_num_threads( omp_num_procs() );//set threads as number of logical cores
    omp_set_num_threads(num_of_threads);
    //cout << omp_get_num_procs() <<endl;
    int total_traingle_cnt = 0;
    #pragma omp parallel for schedule(dynamic  , omp_get_num_procs()) reduction(+:total_traingle_cnt)
    for (long node_id = 0; node_id < graph->getTotalNodes() -2; node_id += 1){
        if(debug)
            logfile << "Thread id : " << this_thread::get_id() << " Node Id : " <<  node_id << endl;
        double startTime = chrono::duration_cast < chrono::milliseconds > (chrono::system_clock::now().time_since_epoch()).count();
        total_traingle_cnt += graph->triangle_count( node_id, &logfile, debug);
        double endTime = chrono::duration_cast < chrono::milliseconds > (chrono::system_clock::now().time_since_epoch()).count();
        if(debug){
            logfile << "Thread id : " << this_thread::get_id() << " Node Id : " <<  node_id << " Time taken : " << endTime - startTime << endl;
            logfile << "Thread id : " << this_thread::get_id() << " Total Traingles : " <<  graph->getNoOfTriangles() << endl;
            logfile << endl;
        }
    }
    cout << total_traingle_cnt << endl;
    //cout << graph->getNoOfTriangles()  << endl;
    if(debug)
        logfile <<"number_of_triangles : " << graph->getNoOfTriangles() << endl;


    chrono::high_resolution_clock::time_point endT = chrono::high_resolution_clock::now();
	double timeTaken = chrono::duration_cast<chrono::milliseconds>(endT-startT).count() ;

	cout << timeTaken << endl;
	if(debug)
    	logfile << "Time Taken : " << timeTaken << endl;



    if(debug)
        logfile.close();
	input_file.close();
	
	return 0;
}




