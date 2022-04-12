#include <iostream>
#include <atomic>
#include <vector>
#include <fstream>
#include <bits/stdc++.h>
#include <map>
#include <unordered_map>
#include <atomic>
#include <pthread.h>
#include <chrono>
#include <regex>
#include <signal.h>

using namespace std;


struct incomplete_node {
    std::atomic<int> state = {0};    
};





class Graph{
    long total_nodes ;

    atomic < long > next_node {
        0
    };

    vector<long> cummulativeIndex;
    vector<long> nodeIndex;
    atomic<long> no_of_triangles{
        0
    };

    struct incomplete_node *incomplete_array;

    bool processing_completed;

    


    public:

    Graph(long total_nodes){
        this->total_nodes = total_nodes;
        this->processing_completed = false;
    }

    ~Graph(){
        
    }

    bool isProcessingCompleted(){
        return this->processing_completed;
    }

    void setProcessingCompleted(bool processing_completed){
        this->processing_completed = processing_completed;
    }
    struct incomplete_node * getIncompleteArray(){
        return this->incomplete_array;
    }

    void setIncompleteArray(struct incomplete_node* arr){
        this->incomplete_array = arr;
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

    void updateNoOfTriangles( int no_of_triangles){
        this->no_of_triangles += no_of_triangles;
    }

    

    void setCummulativeIndex(vector<long> cummulativeIndex){
        this->cummulativeIndex = cummulativeIndex;
    }

    void setNodeIndex(vector<long> nodeIndex){
        this->nodeIndex = nodeIndex;
    }


    long fetchNextNodeAndCheck() {


        long node_id = next_node++;
        int expected = 0;
        int desired = 1;
        if (node_id >= total_nodes - 2)
            return -1;

        while(!this->incomplete_array[node_id].state.compare_exchange_weak(expected, desired, memory_order_release, memory_order_relaxed)){
            node_id = next_node++;
            if (node_id >= total_nodes - 2)
                return -1;
        }

        return node_id;

    }

    int getLeastProcessedNode(){
        vector<long> indexes_to_be_considered;

        for (int i=0;i<total_nodes-2;i++)//last two nodes need not be processed
        {
            if(incomplete_array[i].state.load(std::memory_order_acquire) !=2)
                indexes_to_be_considered.push_back(i);
        }
        // cout << this_thread::get_id() << " Indexes to be considered size :: " << indexes_to_be_considered.size() << endl; 
        // for( long node : indexes_to_be_considered){
        //     cout << node << endl;
        // }
        if (indexes_to_be_considered.size() == 0){
            processing_completed = true;
            return -1;
        }

        srand (time(0));
        long firstInd = rand() % indexes_to_be_considered.size();
        long currInd = firstInd;

        while(true){
            int expected = 0;
            int desired = 1;
            if(this->incomplete_array[indexes_to_be_considered[currInd]].state.compare_exchange_strong(expected, desired,memory_order_release,memory_order_relaxed))
            {
                return indexes_to_be_considered[currInd];
            }
            else if(incomplete_array[indexes_to_be_considered[currInd]].state.load(memory_order_acquire)==1)
                return indexes_to_be_considered[currInd];

            
            currInd = (currInd+1)%indexes_to_be_considered.size();
            if (currInd == firstInd) {
                this->processing_completed = true;
                return -1;
            } 

          
        }
        
    
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
        // this->no_of_triangles+= triangle_cnt;
        return triangle_cnt;
    }

    bool term_condition(){

        for (int ind=0; ind < this-> total_nodes - 2; ind++){
            if (incomplete_array[ind].state.load(memory_order_acquire)!=2)
            {
                // cout << "Node not processed :: " <<  ind << endl;
                return false;
            }
        }

        return true;
    }

};

struct thread_args{
    Graph *graph ;
    fstream * logfile ;
    int thread_num;
    bool debug ;  
    int sleep_time;
    bool term_thread;
    
};


void *thread_funct(void * t_args){
    Graph *graph = ((struct thread_args *)t_args)->graph;
    fstream * logfile = ((struct thread_args *)t_args)->logfile;
    bool debug = ((struct thread_args *)t_args)->debug;
    int thread_num = ((struct thread_args *)t_args)->thread_num;
    int sleep_time = ((struct thread_args *)t_args)->sleep_time;
    bool term_thread = ((struct thread_args *)t_args)->term_thread;

 
    


    long next_node = graph->fetchNextNodeAndCheck();

    // //each thread fetches the nextal available unclaimed node to calculate the triangle count.
    while(next_node != -1){
        if(term_thread){
            if(debug)
                cout << "Thread num :: " << thread_num << " is terminated" << endl;
            pthread_cancel(pthread_self());
            return nullptr;
        }
            
        if(debug)
            (*logfile) << "Thread id : " << this_thread::get_id() << " Node Id : " <<  next_node << endl;
        
        int triangles_cnt = graph->triangle_count( next_node, logfile, debug);
        if(debug){
            (*logfile) << "Thread id : " << this_thread::get_id() << " Node Id : " <<  next_node << endl;
            (*logfile) << "Thread id : " << this_thread::get_id() << " Total Traingles : " <<  graph->getNoOfTriangles() << endl;
        }

        int expected = 1;
        int desired = 2;

        if(sleep_time > 0){
            if(debug)
                (*logfile) << "Thread num :: " << thread_num << " is delayed by " << sleep_time << "ms" << endl;
            this_thread::sleep_for(std::chrono::milliseconds(sleep_time));
        }
        if(graph->getIncompleteArray()[next_node].state.compare_exchange_weak(expected, desired,memory_order_release,memory_order_relaxed)){
            if(debug)
                (*logfile) << "Thread :: " << thread_num << " processed node :: " << next_node << endl;
            graph->updateNoOfTriangles(triangles_cnt);
        }

        next_node = graph->fetchNextNodeAndCheck();
        
        
        

        if (graph->isProcessingCompleted()){
                    return NULL;
        }
        

    }

    if (graph->term_condition()){
        graph->setProcessingCompleted(true);
        return NULL;
    }

    chrono::high_resolution_clock::time_point startT = chrono::high_resolution_clock::now();


    // Helping part

    next_node = graph->getLeastProcessedNode();// returns index or the node id
    while(next_node != -1){
        if(term_thread){
            if(debug)
                cout << "Thread num :: " << thread_num << " is terminated" << endl;
            pthread_cancel(pthread_self());
            return nullptr;
        }
        if(debug)
            (*logfile) << "Thread id : " << this_thread::get_id() << " Node Id : " <<  next_node << endl;
        int triangles_cnt = graph->triangle_count( next_node, logfile, debug);
        double endTime = chrono::duration_cast < chrono::milliseconds > (chrono::system_clock::now().time_since_epoch()).count();
        if(debug){
            (*logfile) << "Thread id : " << this_thread::get_id() << " Node Id : " <<  next_node << " Time taken : " << endl;
            (*logfile) << "Thread id : " << this_thread::get_id() << " Total Traingles : " <<  graph->getNoOfTriangles() << endl;
        }
        int expected = 1;
        int desired = 2;
        if(sleep_time > 0){
            if(debug)
                (*logfile) << "Thread num :: " << thread_num << " is delayed by " << sleep_time << "ms" << endl;
            this_thread::sleep_for(std::chrono::milliseconds(sleep_time));
        }
        if(graph->getIncompleteArray()[next_node].state.compare_exchange_weak(expected, desired,memory_order_release,memory_order_relaxed)){
            if(debug)
                (*logfile) << "Thread :: " << thread_num << "(helper)" << " processed node :: " << next_node << endl;
            graph->updateNoOfTriangles(triangles_cnt);
        }

        if(graph->isProcessingCompleted()){
            return nullptr;
        }

        next_node = graph->getLeastProcessedNode();// returns index or the node id
        
    }
    chrono::high_resolution_clock::time_point endT = chrono::high_resolution_clock::now();

    // // this_thread::sleep_for(std::chrono::milliseconds(100000000));
    return nullptr;
}


 

int main(int argc, char** argv){



    string logFileName = "../output/logfileParr.txt";
	string dataset = "../datasets/graph500-scale18.txt";
    string numberOfThreadsStr = "16";
    string term_thread_cnt_str = "0";
    string sleep_time_str = "45";
    string sleep_thread_cnt_str = "14";
	bool debug = false;
	if(argc > 1){
		logFileName = argv[1]  ;
		dataset = argv[2] ; 
        numberOfThreadsStr = argv[3];
        term_thread_cnt_str = argv[4];
        sleep_time_str = argv[5];
        sleep_thread_cnt_str = argv[6];
        if(argc == 8){
			 if(argv[7] == "debug"){
            	debug = true;
       		 }
		}
	}
    
    fstream logfile;
    if(debug)
        logfile.open(logFileName,ios::out);
    ifstream input_file;
	input_file.open(dataset,ios::in);
	
    int term_thread_cnt = stoi(term_thread_cnt_str);
    int sleep_time = stoi(sleep_time_str);


    int   num_of_threads = stoi(numberOfThreadsStr);

    int sleep_thread_cnt = stoi(sleep_thread_cnt_str);
	

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


    pthread_t threads[num_of_threads];

    Graph *graph = new Graph(total_nodes);
    graph->setCummulativeIndex(cummulativeIndex);
    graph->setNodeIndex(nodeIndex);

    struct incomplete_node *temp_arr;
    temp_arr = (struct incomplete_node *) calloc (total_nodes, sizeof (struct incomplete_node));

    graph->setIncompleteArray(temp_arr);
    struct thread_args t_args[num_of_threads];

    set<int> term_thread_nums;

    srand(time(0));
    while(term_thread_cnt--){
        int thread_num = rand() % num_of_threads;
        while(term_thread_nums.find(thread_num) != term_thread_nums.end()){
            thread_num = rand() % num_of_threads;
        }
        term_thread_nums.insert(thread_num);
    }

    set<int> sleep_thread_nums;

    srand(time(0));
    while(sleep_thread_cnt--){
        int thread_num = rand() % num_of_threads;
        while(sleep_thread_nums.find(thread_num) != sleep_thread_nums.end()){
            thread_num = rand() % num_of_threads;
        }
        sleep_thread_nums.insert(thread_num);
    }




	chrono::high_resolution_clock::time_point startT = chrono::high_resolution_clock::now();

    

    for( int i=0;i < num_of_threads ;i++){
        if(term_thread_nums.find(i) != term_thread_nums.end()){
            t_args[i].term_thread = true;
            if(debug)
                logfile << "Thread num :: " << i << " will be terminated" << endl;
        }
        else{
            t_args[i].term_thread = false;
        }

        if(sleep_thread_nums.find(i) != sleep_thread_nums.end()){
            t_args[i].sleep_time = sleep_time;
            if(debug)
                logfile << "Thread num :: " << i << " will be slow" << endl;
        }
        else{
            t_args[i].sleep_time = 0;
        }

    

    
        
        t_args[i].logfile = &logfile;
        t_args[i].graph = graph;
        t_args[i].debug = debug;
        t_args[i].thread_num = i;
        pthread_create(&threads[i], NULL, thread_funct, &t_args[i]);
        
    }
    int temp_up = false;
    // this_thread::sleep_for(std::chrono::milliseconds(10000));
    while(!graph->isProcessingCompleted()){
        cout << "";//this is added else busy wait will be removed in compiler optimization
    }
    
    cout << graph->getNoOfTriangles()  <<endl;
    if(debug)
        logfile <<"number_of_triangles : " << graph->getNoOfTriangles() << endl;


    chrono::high_resolution_clock::time_point endT = chrono::high_resolution_clock::now();
	double timeTaken = chrono::duration_cast<chrono::milliseconds>(endT-startT).count() ;

	cout << timeTaken << endl;
	if(debug)
    	logfile << "Time Taken : " << timeTaken << endl;




    for(int i=0 ; i< num_of_threads;i++){
        pthread_cancel(threads[i]);
    }

    for(int i=0 ; i< num_of_threads;i++){
        pthread_join(threads[i], NULL);
    }
    
    
    



    if(debug)
        logfile.close();
	input_file.close();
    free(temp_arr);
	return 0;
}




