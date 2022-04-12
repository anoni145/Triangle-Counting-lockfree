import os
import sys
import shlex
import subprocess as sp
from subprocess import Popen
from datetime import datetime
from csv import writer
import random

now = datetime.now() # current date and time

date_time_obj =now.strftime('%Y_%m_%d_%H_%M_%S') 


threads = [8,16] 
input_files = ["14.txt"]
parr_programs = ["tri_count_parr_dynamic.cpp","tri_count_parr_helper.cpp","tri_count_parr_static.cpp"]
seq_program = "tri_count_seq.cpp"
debug = False
output_file = './output/result_' + date_time_obj + '.csv'
seq_iterations = 5
parr_iterations = 5



with open( output_file, 'w+') as f_object:
  lst = ["Input Graph" ,  "Triangle Cnt(Seq)" , "Seq Time" ]
  for parr_program in parr_programs:
      file_name = parr_program.split(".")[0]
      parr_ver = file_name.split("_")[-1]
      lst.extend(["parr time(" + parr_ver + ")" , ""])
      for i in range(len(threads) - 1):
          lst.extend(["",""])

  writer_object = writer(f_object)

  writer_object.writerow(lst)
  lst = ["", "" , 1 ] 
  for parr_program  in parr_programs:
    for thread_cnt in threads:
        lst.append(thread_cnt)
        lst.append("Speedup")
  writer_object.writerow(lst)

 

def random_avg_val(lst, rand_cnt):
  return random.sample(lst, rand_cnt)


script_log_file = "./script_log/log_" + date_time_obj + ".txt"
with open(script_log_file, 'w+') as log_f_object:
    with open(output_file, 'a+') as f_object:
        writer_object = writer(f_object)


        for j in range (len(input_files)) :
            input_file = input_files[j]
            lst = []
            lst.append(input_file.split(".")[0])
            print("Dataset : " + input_file.split(".")[0] , file = log_f_object,flush = True)

            data_set_file = "../Datasets/" + input_file

            cmd1 = "g++ -std=c++17 -O3 -o ./algorithms/seq.out ./algorithms/" + seq_program
            proc = sp.Popen(cmd1.split())
            proc.wait()


            cmd2 = "./algorithms/seq.out " + "./output/logseq_" + date_time_obj +".txt "  + "./datasets/" +input_file  

            if debug:
                cmd2 += " debug"

      
            print("SEQUENTIAL ",file = log_f_object,flush = True)
            number_of_triangles_seq = 0
            seq_time_taken_list = []


            #update sequential iteration
            for i in range(seq_iterations):
                print("Iteration : "+ str(i),file = log_f_object,flush = True)
                proc = sp.Popen(cmd2.split() ,stdout=sp.PIPE)
                std_output, std_err = proc.communicate()
                if std_err is not None:
                    raise Exception(std_err)
                number_of_triangles_seq, time_taken_seq, _ = std_output.decode().split('\n')
                if not number_of_triangles_seq:
                    lst.append("")
                    print("No o/p at in seq execution. Iteration : "+ str(i), file = log_f_object,flush = True)
                    continue
                else: 
                    print("No of Traingles(Seq) : " , str(number_of_triangles_seq), file = log_f_object,flush = True)
                    if not time_taken_seq:
                        lst.append("")
                        print("No timetaken in seq o/p" , file = log_f_object,flush = True)
                        continue
                    else:
                        seq_time_taken_list.append(float(time_taken_seq))
                print(file = log_f_object,flush = True)
      
            lst.append(number_of_triangles_seq)
            time_taken_seq_avg = 0
            rand_seq_time_taken_list = seq_time_taken_list[2:]
            time_taken_seq_avg = int(sum(rand_seq_time_taken_list)/len(rand_seq_time_taken_list))
            lst.append(time_taken_seq_avg)
            print("Time Taken : " + str(time_taken_seq_avg), file = log_f_object,flush = True)
                
            print(file = log_f_object,flush = True)
            print(file = log_f_object,flush = True)
            print(file = log_f_object,flush = True)

            #parrallel part
            print("PARALLEL : ",file = log_f_object,flush = True)
            for parr_program in parr_programs:
                file_name = parr_program.split(".")[0]
                parr_ver = file_name.split("_")[-1]
                print("Parallel Version : " + parr_ver,file = log_f_object,flush = True)
                cmd1 = "g++ -std=c++17 -pthread -O3 -o ./algorithms/parr.out ./algorithms/" + parr_program
                proc = sp.Popen(cmd1.split())
                proc.wait()

                number_of_triangles_parr = 0
                for thread_cnt in threads :
                    print("Thread cnt : " + str(thread_cnt),file = log_f_object,flush = True)
                    cmd2 = "./algorithms/parr.out " + "./output/logparr_" + date_time_obj +".txt "  + "./datasets/" +input_file + " " + str(thread_cnt) 
                    
                    if parr_ver == "helper" :
                        cmd2 += " 0 0" #update this to change term thread count and sleep thread cnt respectively

                    if debug:
                        cmd2 += " debug"
                    time_taken_par_list = []

                    #update parallel iterations
                    for i in range(parr_iterations):
                        print("Iteration : "+ str(i),file = log_f_object,flush = True)
                        proc = sp.Popen(cmd2.split() ,stdout=sp.PIPE)
                        std_output, std_err = proc.communicate()
                        if std_err is not None:
                            raise Exception(std_err)
                        number_of_triangles_parr, time_taken_par, _ = std_output.decode().split('\n')
                        if not number_of_triangles_parr:
                            lst.append("")#empty for that thread
                            print("No o/p for parr for thread cnt : " + str(thread_cnt) + " and  Iteration :  " + str(i),file = log_f_object,flush = True)
                            continue
                        print("No of triangeles(parr) : " + str(number_of_triangles_parr),file = log_f_object,flush = True)

                        if not time_taken_par:
                            lst.append("")
                            print("No timetake in o/p for parr for thread cnt : " + str(thread_cnt)+ " and  Iteration : " + str(i),file = log_f_object,flush = True)
                            continue
                        print("Time Taken : " + str(time_taken_par),file = log_f_object,flush = True)
                        time_taken_par_list.append(float(time_taken_par))
                        print(file= log_f_object,flush = True)
                        print(file= log_f_object,flush = True)
                        
                    time_taken_par_avg = 0
                    if len(time_taken_par_list)!=0:
                        rand_iter_cnt = len(time_taken_par_list)
                    if(rand_iter_cnt > 5):
                        rand_iter_cnt -= 2
                    rand_time_taken_par_list = time_taken_par_list[2:]
                    time_taken_par_avg = int(sum(rand_time_taken_par_list)/len(rand_time_taken_par_list))
                    lst.append(time_taken_par_avg)
                    lst.append(round(float(time_taken_seq)/(float(time_taken_par_avg)+1e-9),4))
                    print("\n\n",file= log_f_object,flush = True)
            writer_object.writerow(lst)
        print(file= log_f_object,flush = True)
        print(file= log_f_object,flush = True)
        print(file= log_f_object,flush = True)







