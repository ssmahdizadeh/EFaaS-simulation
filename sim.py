import simpy
import numpy as np
import pandas as pd
import random
import time
import json
import shared_params as sp
import os
import efaas as ef
import HPFM as mt
#import LSFM  as mt
#import SD as mt
#import LFU as mt
 


random.seed(sp.random_seed)

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

def write_excel(data,filename,sheet):
    if os.path.isfile(filename+".xlsx"):
        with pd.ExcelWriter(filename+".xlsx", mode='a') as writer:
            data.to_excel(writer, sheet_name=f'Sheet{sheet}', index=False)
    else:
        with pd.ExcelWriter(filename+".xlsx") as writer:
            data.to_excel(writer, sheet_name=f'Sheet{sheet}', index=False)

def generate_log(allocation_status, file_num , num_user , sim_repeat ):
    df0 = pd.DataFrame(sp.logs["logs"], columns=['ScheduleRound','time','desc'])
    write_excel(df0,"output_"+str(sp.cont.name)+"_logs"+str(file_num),str(num_user)+"_"+str(sim_repeat+1))

    df = pd.DataFrame(sp.logs["allocations"], columns=['ScheduleRound','UserID', 'task_name' , 'instance_name','function_name' , 'EdgeServer','ExecutionTime', 'StartTime', 'EndTime','ContainerStatus'])
    write_excel(df,"output_"+str(sp.cont.name)+"_allocations"+str(file_num),str(num_user)+"_"+str(sim_repeat+1))

    df2 = pd.DataFrame(sp.logs["containers_info"], columns=['ServerID', 'Id' , 'Function_name','StartTime'])
    write_excel(df2,"output_"+str(sp.cont.name)+"_containers"+str(file_num),str(num_user)+"_"+str(sim_repeat+1))
        
    df3 =  pd.DataFrame(sp.logs["workflow_info"] , columns=['UserID','job_name', 'AServersNum' , 'TasksNum','instances_num','StartTime','EndTime','OptimalTime','makespan','DiffOpt'])
    for id , wf in df3.iterrows() :
        user_allocations = df.loc[df['UserID'] == wf['UserID']]
        df3.at[id, 'L2'] = (user_allocations["EndTime"]- user_allocations["StartTime"] - user_allocations["ExecutionTime"]).pow(2).mean()
    write_excel(df3,"output_"+str(sp.cont.name)+"_workflows"+str(file_num),str(num_user)+"_"+str(sim_repeat+1))

           
    temp = {}
    count_allocation_status = df['ContainerStatus'].value_counts()
    for value, count in count_allocation_status.items():
        temp[value] = count

    temp['min_makespan'] = df3['makespan'].min()
    temp['max_makespan'] = df3['makespan'].max()
    temp['avg_makespan'] = df3['makespan'].mean()
    temp['std_makespan'] = df3['makespan'].std()
    temp['min_OptimalTime'] = df3['OptimalTime'].min()
    temp['max_OptimalTime'] = df3['OptimalTime'].max()
    temp['avg_OptimalTime'] = df3['OptimalTime'].mean()
    temp['std_OptimalTime'] = df3['OptimalTime'].std()
    temp['min_DiffOpt'] = df3['DiffOpt'].min()
    temp['max_DiffOpt'] = df3['DiffOpt'].max()
    temp['avg_DiffOpt'] = df3['DiffOpt'].mean()
    temp['avg_DiffOpt'] = df3['DiffOpt'].mean()
    temp['std_DiffOpt'] = df3['DiffOpt'].std()
    temp['L2'] =(df3['DiffOpt'].pow(2)).mean()
    temp['sum_tasks'] = df3['TasksNum'].sum()
    temp['sum_instances'] = df3['instances_num'].sum()

    allocation_status.append(temp) 


file_num = random.randint(0,1000)
print("out_file_num: " , file_num)

sp.parse_instance_file()
sp.locate_edge_servers()

for sp.scenario in range(len(sp.USER_ENTRY_RATES)):

    allocation_status = []
    for sim_repeat in range(sp.SIM_REPEAT):
        sp.reset_logs()

        env = simpy.Environment()
        
        edge_servers = [ef.EdgeServer(env, i) for i in range(sp.NUM_EDGE_SERVERS)]
        users = [ef.User(env,  i) for i in range(sp.USERS_INITIAL_NUMBER)]
        
        sp.cont = mt.Controller(env,users,edge_servers)
        
        env.run(until=sp.SIM_TIME)

        generate_log(allocation_status, file_num ,sp.scenario, sim_repeat )       

        del env
        del edge_servers
        del users
        sp.cont = None
    
    count_col1_dataframe =  pd.DataFrame( allocation_status, columns=['user_num','warm_allocated', 'cold_allocated' ,'failed','cloud','min_makespan',
                                                                    'max_makespan','avg_makespan','std_makespan','min_OptimalTime','max_OptimalTime','avg_OptimalTime',
                                                                    'std_OptimalTime','min_DiffOpt','max_DiffOpt','avg_DiffOpt','std_DiffOpt','L2','sum_tasks','sum_instances'])
    count_col1_dataframe.fillna(0,inplace=True)
    write_excel(count_col1_dataframe,"output_"+str(mt.method_name)+"_"+str(file_num),"_"+str(sp.scenario))
    del allocation_status

print( 'done')
