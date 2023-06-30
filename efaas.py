import simpy
import numpy as np
import pandas as pd
import random
import time
import json
import shared_params as sp
import os

random.seed(sp.random_seed)

class Task:
    def __init__(self, name, function_name,instances):
        self.name = name 
        self.function_name = function_name
        self.instances = [] 
        self.dep_list = []
        self.id = self.name[1:].split("_")[0]
        self.dep_list = self.name[1:].split("_")
        self.dep_list.pop(0)
        if '_' in self.name:
            self.status  = 'not ready'
        else:
            self.status = 'ready' 
        self.parse_instance_list(instances, self.status)    
        self.data_size = max(p.data_size for p in self.instances)
        self.ex_time = max(p.ex_time for p in self.instances)

    def parse_instance_list(self,instances ,status) :
        for index, instance in instances.iterrows():
            inst_obj = Instance(instance["instance_name"],self.function_name, instance["cpu_avg"], instance["mem_avg"],status)
            self.instances.append(inst_obj)

    def set_instances_status(self, status):
        for inst in self.instances:
            inst.status = status            

class Instance:
    def __init__(self, name ,function_name, cpu_avg ,mem_avg, status):
        self.name = name
        self.function_name = function_name
        self.cpu_avg = cpu_avg
        self.mem_avg = mem_avg
        self.status = status
        f = sp.get_function(function_name)
        if f :
            self.ex_time = f["ex_time"]
            self.data_size = f["data_size"]
        else :
            self.ex_time = self.data_size = 0

class User:

    def __init__(self, env, id):
        self.env = env
        self.id = id
        self.start_time = False
        self.status = 'ready'
        self.x = random.uniform(0, sp.REGION_SIZE)
        self.y = random.uniform(0, sp.REGION_SIZE)
        self.accessible_servers = sp.get_accessible_servers(self.x,self.y)
        sp.logs["logs"].append({'ScheduleRound' : 0,'time' : self.env.now ,
                                    'desc' : " user : "+str(self.id) + "accessible_servers"+str(self.accessible_servers)})
        #t_rate = np.array([obj['t_rate'] for obj in self.accessible_servers])
        #self.mean_t_rate = np.mean(t_rate)
        self.tasks = []
        self.process_job(sp.get_random_job())
        self.calculate_urank() 
        self.random_order = random.random()
        return None        
        
    def get_task_es_offloding_time(self,task):
        return task.ex_time + round(task.data_size/self.accessible_servers[0]["t_rate"],2)
        #return task.ex_time + round(task.data_size/self.mean_t_rate,2)

    def get_instance_es_offloding_time(self,instance):
        #print("ex : "+str(instance.ex_time)+" net:"+str(round(instance.data_size/self.accessible_servers[0]["t_rate"],2)))
        return instance.ex_time + round(instance.data_size/self.accessible_servers[0]["t_rate"],2)
        #return instance.ex_time + round(instance.data_size/self.mean_t_rate,2)

    def process_job(self,instances):
        self.job_name = instances["job_name"].iloc[0]
        tasks = instances.groupby('task_name')
        tasks = list(tasks.groups.keys())
        for task_name in tasks:
            instance_list = instances[instances['task_name'] == task_name]
            task = Task(task_name,self.job_name+task_name,instance_list)
            self.tasks.append(task)    
        return True
                        
    def calculate_urank(self):
        alloc = []
        #for i, task in enumerate(self.tasks):
        for task in self.tasks:
            s = [st for st in self.tasks if task.id in st.dep_list]
            if not s :
                for instance in task.instances : 
                    instance.es_time_cost =  self.get_instance_es_offloding_time(instance)
                    instance.urank = instance.es_time_cost
                task.urank = self.get_task_es_offloding_time(task)
                alloc.append(task.id)
        #print("alloc",alloc)        
        newalloc = True        
        while  newalloc:
            newalloc = False
            for task in self.tasks:
                s = [st.id for st in self.tasks if task.id in st.dep_list]
                #print("s1",s)
                if task.id not in alloc and set(s).issubset(set(alloc)):
                    newalloc = True
                    s = [st for st in self.tasks if task.id in st.dep_list]
                    #for w in s :
                    #    print("s2",w.name)
                    #task['urank'] = task['tt'] + task['et'] + max( st['urank'] for st in s)
                    urank = max( st.urank for st in s)
                    task.urank = self.get_task_es_offloding_time(task) + urank
                    
                    for instance in task.instances: 
                        instance.es_time_cost =  self.get_instance_es_offloding_time(instance)
                        instance.urank = instance.es_time_cost + urank
                    alloc.append(task.id)            

    def update_ready_tasks(self,done_task):
        for task in self.tasks:
            if(task.status == 'not ready'):
                check_dep = True
                for dep_task in task.dep_list:
                    n_done = [nd for nd in self.tasks if nd.id == dep_task and nd.status != 'done']
                    if len(n_done)>0 :
                        check_dep = False
                        break
                if check_dep is True  :
                    task.status = 'ready' #if random.random() > sp.LOCAL_TASK_PROBABILITY  else 'local'
                    task.set_instances_status('ready')
                    #if(task.status == 'local'):
                    #        self.env.process(self.execute_locally(task['id'], task['function_id'], task['et']))
                
    def get_ready_tasks(self):
        if self.status == 'done':
            return []
        if self.start_time is False :
            self.start_time = self.env.now
        ready_list = []
        for task in self.tasks:
            if task.status == 'ready':
               task.status = 'requested'
               for instance  in task.instances :
                   instance.status = 'requested'
                   ready_list.append({"user_id" : self.id ,"task_name": task.name, "instance_name" : instance.name,
                                "function_name" : task.function_name ,"urank" : instance.urank ,"es_time_cost" :instance.es_time_cost
                                ,"random_order" : self.random_order})      
        s_urank = sum(d["urank"] for d in ready_list)
        token = int(100*len(ready_list))
        for ready_instance in ready_list:
            ready_instance["token"] = round(token * ready_instance["urank"]/s_urank,2)  
        return ready_list
    

    def terminate_task(self,task_name,instance_name):
        for task in self.tasks:
            if task.name== task_name and task.status == 'requested' :
                for instance  in task.instances:
                    if instance.name == instance_name :
                        instance.status = 'done'
                all_done = True        
                for instance  in task.instances:        
                    if instance.status != 'done':
                        all_done = False
                if all_done is True:
                    task.status = 'done'
                    self.update_ready_tasks(task.id)
                break
        if self.status == 'done':
            return True
        if self.done_all() :
            self.status = 'done'
            optimal_time = max(task.urank for task in self.tasks)
            end_time = self.env.now
            sp.logs["workflow_info"].append({'UserID' : self.id,'job_name' : self.job_name, 'AServersNum' : len(self.accessible_servers), 'TasksNum' : len(self.tasks),
                                             'instances_num' : sum(len(i.instances) for i in self.tasks),
                                             'StartTime': self.start_time,'EndTime': end_time,'OptimalTime' : optimal_time ,
                                             'makespan': end_time-self.start_time , 'DiffOpt' : (end_time-self.start_time)-optimal_time})
            #sp.cont.new_user(self.id)

    def done_all(self):
        for task in self.tasks:
            if task.status != 'done':
                return False
        return  True    

    def offloadding_to_cloud(self,task_name,instance_name):
        for task in self.tasks :
            if task.name != task_name :
                continue
            for instance in task.instances :
                if instance.name != instance_name :
                    continue
                start_time = self.env.now
                #time_cost = instance.es_time_cost*sp.CLOUD_COEF
                time_cost = round ( instance.ex_time * (1+random.uniform(-1*sp.EXE_ERROR_RATE, sp.EXE_ERROR_RATE)) + instance.data_size / 0.2 , 4)
                yield self.env.timeout(time_cost)
                end_time = self.env.now
                sp.logs["allocations"].append({'ScheduleRound':sp.cont.schedule_round ,'UserID': self.id,"task_name": task_name,"instance_name" :instance_name,
                                             'function_name' : self.job_name+task_name,
                                            'EdgeServer': 'Cloud','ExecutionTime':  instance.es_time_cost, 
                                            'StartTime': start_time, 'EndTime': end_time,"ContainerId" :0 ,
                                            "ContainerStatus":"cloud"})
                self.terminate_task(task_name,instance_name)

    def not_allocated(self,task_name,instance_name):
        self.env.process(self.offloadding_to_cloud(task_name,instance_name))
        
class Container:
    def __init__(self, env, id, function_id,server_id ,status):
        self.env = env
        self.id = id
        self.function_id = function_id
        self.function_name = sp.functions[function_id]["name"]
        self.server_id = server_id
        self.status = status
        self.LRU = self.env.now
       
    def run(self,user_id,task_name,instance_name,ex_time_cost,tr_time_cost,es_time_cost):
        round = sp.cont.schedule_round
        start_time = self.env.now
        status2 = self.status
        if self.status == 'warm' or self.status =='warm_allocated':
            yield self.env.timeout(sp.WARM_START_TIME)
        else:
            if self.status == 'cold' or self.status =='cold_allocated':
                yield self.env.timeout(sp.COLD_START_TIME)
            else :
                return False    
            #self.status = 'warm' if  self.status == 'cold' else 'warm_allocated' 
        self.status = 'busy'
        self.ex_time_cost = ex_time_cost
        time_cost = ex_time_cost+tr_time_cost
        yield self.env.timeout(time_cost)
        sp.cont.users[user_id].terminate_task(task_name,instance_name)
        self.status = 'warm'
        self.LRU = self.env.now
        end_time = self.env.now
        sp.logs["allocations"].append({'ScheduleRound' : round ,'UserID': user_id,
                                       "task_name": task_name,'instance_name':instance_name,'function_name':self.function_name,
                                       'EdgeServer': self.server_id,'ExecutionTime':es_time_cost, 
                                       'StartTime': start_time, 'EndTime': end_time,
                                       "ContainerId": self.id,"ContainerStatus": status2})

class EdgeServer:
    def __init__(self, env, id):
        self.env = env
        self.id = id
        self.cpu_cores = sp.get_random_egde_size()
        sp.logs["logs"].append({'ScheduleRound' : 0,'time' : self.env.now ,
                                    'desc' : " egde_size : "+str(self.cpu_cores)})
        #random.randint(sp.EDGE_SIZE[sp.scenario][1],sp.EDGE_SIZE[sp.scenario][1])
        self.memory = random.randint(sp.MIN_EDGE_MEMORY,sp.MAX_EDGE_MEMORY)
        #self.speed = round(random.uniform(sp.MIN_SERVER_SPEED_COEF, sp.MAX_SERVER_SPEED_COEF),1)
        self.assigned_cpu = 0
        self.assigned_mem = 0 
        self.containers = []
        self.request_list = []
        new_container = True
        while new_container is True:
            function_id = random.randint(0,len(sp.functions)-1)
            function_num =  random.randint(1,5)
            cpu_avg = sp.functions[function_id]["cpu_avg"]
            mem_avg = sp.functions[function_id]["mem_avg"]
            new_container = False
            for i in range(function_num) :
                if self.assigned_cpu + cpu_avg < self.cpu_cores and self.assigned_mem + mem_avg < self.memory :
                    self.containers.append(Container(env,len(self.containers),function_id,self.id,'warm'))
                    self.assigned_cpu += cpu_avg
                    self.assigned_mem += mem_avg
                    sp.logs["containers_info"].append({'ServerID' :id, 'Id':str(len(self.containers)-1) , 
                                               'Function_name' :sp.functions[function_id]["name"] , 'StartTime' : self.env.now})
                    new_container = True
               
        self.processes = {}
    
    def allocate_container(self,user_id,task_name,instance_name,container_id,ex_time_cost,tr_time_cost,es_time_cost):
        #[container for container in self.containers if container.container_id == container_id]
        self.containers[container_id].status = 'warm_allocated'
        if  (self.cpu_cores < sum(sp.functions[co.function_id]["cpu_avg"] for co in self.containers if  co.status == 'busy') + sp.functions[self.containers[container_id].function_id]["cpu_avg"]
                or self.memory < sum(sp.functions[co.function_id]["mem_avg"] for co in self.containers if  co.status == 'busy') + sp.functions[self.containers[container_id].function_id]["mem_avg"] ):
            return False
        self.env.process(self.containers[container_id].run(user_id,task_name,instance_name,ex_time_cost,tr_time_cost,es_time_cost))
        return True
    
    def release_LRU_container(self,function_name):
        warm_containers = [container for container in self.containers if container.status == 'warm']
        if not warm_containers:
            return False
        warm_containers.sort(key=lambda x: x.LRU)
        function = [i for i in sp.functions if i["name"] == function_name]
        if len(function) < 1:
            return False 
        
        function = function[0]
        cpu_avg = function["cpu_avg"]
        mem_avg = function["mem_avg"]
        m_cpu_avg = 0
        m_mem_avg = 0
        for container in warm_containers :
            m_cpu_avg += sp.functions[container.function_id]["cpu_avg"]
            m_mem_avg += sp.functions[container.function_id]["mem_avg"]
        
        if m_cpu_avg < cpu_avg or m_mem_avg < mem_avg :
            return False
        
        new_container = False 
        for container in warm_containers :
            self.assigned_cpu -= sp.functions[container.function_id]["cpu_avg"]
            self.assigned_mem -= sp.functions[container.function_id]["mem_avg"]
            self.containers[container.id].status = 'released'
            
            if self.assigned_cpu + cpu_avg < self.cpu_cores and self.assigned_mem + mem_avg < self.memory :
                
                self.containers.append(Container(self.env,len(self.containers),function["id"],self.id,'cold'))
                self.assigned_cpu += cpu_avg
                self.assigned_mem += mem_avg
                new_container = True 
                break
        if new_container is True :
            sp.logs["containers_info"].append({'ServerID' :self.id, 'Id': len(self.containers)-1 , 
                                               'Function_name' : self.containers[len(self.containers)-1].function_name, 'StartTime' : self.env.now})
            return len(self.containers)-1
        else :
            return False

    def LFU_rank(self) :
        warm_containers = [container for container in self.containers if container.status == 'warm']
        if not warm_containers:
            return False
               
        for warm_container in warm_containers:
            total_ex_time = sum(co.ex_time_cost for co in self.containers if  co.function_name == warm_container.function_name
                                 and co.status == 'busy')
            warm_container.LFU_rank = total_ex_time

    def LFU_provisioning(self,request_list):
        print("LFU_provisioning")
        warm_containers = [container for container in self.containers if container.status == 'warm']
        if not warm_containers:
            return False
               
        for request in self.request_list:
            total_ex_time = sum( 1 for co in self.request_list if 
                                request["function_name"] == co["function_name"]) * sp.get_function(request["function_name"])["ex_time"]
            request["LFU_rank"] = total_ex_time

        warm_containers.sort(key=lambda x: x.LFU_rank)
        self.request_list.sort(key=lambda x: x["LFU_rank"],reverse = True)
        for request in self.request_list:
            function = [i for i in sp.functions if i["name"] == request["function_name"]]
            if len(function) < 1:
                continue
            function = function[0]
            new_container = False
            for container in warm_containers:
                if request["LFU_rank"] > container.LFU_rank and self.containers[container.id].status == 'warm' and new_container is False :
                    self.assigned_cpu -= sp.functions[container.function_id]["cpu_avg"]
                    self.assigned_mem -= sp.functions[container.function_id]["mem_avg"]
                    self.containers[container.id].status = 'released'
                    if self.assigned_cpu + function["cpu_avg"] < 1.5*self.cpu_cores and self.assigned_mem + function["mem_avg"] < 1.5*self.memory :
                        self.containers.append(Container(self.env,len(self.containers),function["id"],self.id,'cold'))
                        print("LFU_provisioning2")
                        self.assigned_cpu += function["cpu_avg"]
                        self.assigned_mem += function["mem_avg"]
                        new_container = True 
                    
                else:
                    break
            if new_container is False:
                break  

        for container in self.containers:
            container.LFU_rank = 0
        self.request_list = []     
    
    def release_container(self,function_name):
        warm_containers = [container for container in self.containers if container.status == 'warm']
        if not warm_containers:
            return False
        function = [i for i in sp.functions if i["name"] == function_name]
        if len(function) < 1:
            return False 
        function = function[0]
        cpu_avg = function["cpu_avg"]
        mem_avg = function["mem_avg"]
        m_cpu_avg = 0
        m_mem_avg = 0
        for container in warm_containers :
            m_cpu_avg += sp.functions[container.function_id]["cpu_avg"]
            m_mem_avg += sp.functions[container.function_id]["mem_avg"]
        
        if m_cpu_avg < cpu_avg or m_mem_avg < mem_avg :
            return False
        
        new_container = False 
        for container in warm_containers :
            self.assigned_cpu -= sp.functions[container.function_id]["cpu_avg"]
            self.assigned_mem -= sp.functions[container.function_id]["mem_avg"]
            self.containers[container.id].status = 'released'
            
            if self.assigned_cpu + cpu_avg < self.cpu_cores and self.assigned_mem + mem_avg < self.memory :
                
                self.containers.append(Container(self.env,len(self.containers),function["id"],self.id,'cold'))
                self.assigned_cpu += cpu_avg
                self.assigned_mem += mem_avg
                new_container = True 
                break
        if new_container is True :
            sp.logs["containers_info"].append({'ServerID' :self.id, 'Id': len(self.containers)-1 , 
                                               'Function_name' : self.containers[len(self.containers)-1].function_name, 'StartTime' : self.env.now})
            return len(self.containers)-1
        else :
            return False
      
    def initial_new_container(self,user_id,task_name,instance_name,container_id,ex_time_cost,tr_time_cost,es_time_cost):
        self.containers[container_id].status = 'cold_allocated'
        if  (self.cpu_cores < sum(sp.functions[co.function_id]["cpu_avg"] for co in self.containers if  co.status == 'busy') + sp.functions[self.containers[container_id].function_id]["cpu_avg"]
                or self.memory < sum(sp.functions[co.function_id]["mem_avg"] for co in self.containers if  co.status == 'busy') + sp.functions[self.containers[container_id].function_id]["mem_avg"] ):
            return False
        self.env.process(self.containers[container_id].run(user_id,task_name,instance_name,ex_time_cost,tr_time_cost,es_time_cost))
        return True
  