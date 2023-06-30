import simpy
import numpy as np
import pandas as pd
import random
import time
import json
import shared_params as sp
import os
import efaas as ef
import itertools

random.seed(sp.random_seed)

method_name = str(sp.CONFIG_CODE)+"_SD"     

class Controller(object):
    def __init__(self, env, users ,edge_servers):
        self.env = env
        self.action = self.env.process(self.scheduler())
        self.users_entry = self.env.process(self.user_entry())
        self.name = method_name
        self.schedule_round = 0
        self.users = users
        self.edge_servers = edge_servers
        self.default_user_num = len(users)
    
    def scheduler(self):
        while True:
            self.schedule_round += 1
            requests = []
            sp.logs["logs"].append({'ScheduleRound' : self.schedule_round,'time' : self.env.now ,
                                    'desc' : " number of active users"+str(sum(1 for u in self.users if u.status!='done'))})
            for user in self.users:
                # "user_id","task_name","instance_name","function_name","token"
                user_requests = user.get_ready_tasks()
                if len(user_requests)>0 :
                    for request in user_requests:
                        requests.append(request)
            if len(requests) > 0:
                 self.allocate_resources(requests)
            yield self.env.timeout(sp.SCHEDULER_INTERVAL)
            
    def user_entry(self) :
        lam = sp.USER_ENTRY_RATES[sp.scenario]
        while True:
            inter_arrival_time = np.random.poisson(lam)
            yield self.env.timeout(inter_arrival_time)
            self.users.append(ef.User(self.env,len(self.users)))

    def allocate_resources(self, requests):
        
        requests.sort(key=lambda x: (x["random_order"], x['user_id']))
        for request in requests:
            user = self.users[request['user_id']]
            request["available_resource"] = []
            request["status"] = 'processing'
            for accessible_server in user.accessible_servers:
                available_resource = [container for container in  self.edge_servers[accessible_server["server_id"]].containers
                                       if container.function_name == request['function_name'] and (container.status == 'warm' or container.status == "cold")]
                                       
                if len(available_resource) > 0:
                    available_resource = available_resource[0]
                    ex_time_cost = round(sp.functions[available_resource.function_id]['ex_time']*(1+random.uniform(-1*sp.EXE_ERROR_RATE, sp.EXE_ERROR_RATE)),4)
                    tr_time_cost = round(sp.functions[available_resource.function_id]['data_size'] / accessible_server["t_rate"],4)
                    self.edge_servers[accessible_server["server_id"]].allocate_container(request['user_id'],request['task_name'],request['instance_name'],
                                                                    available_resource.id,ex_time_cost,tr_time_cost,request['es_time_cost'])
                    request["status"] = 'allocated'
                    break
                   
            if request["status"] != 'allocated':
               for accessible_server in user.accessible_servers:
                    container_id = self.edge_servers[accessible_server["server_id"]].release_container(request['function_name'])
                
                    if container_id is not False :
                        func = sp.get_function(request['function_name'])
                        ex_time_cost = round(func['ex_time']*(1+random.uniform(-1*sp.EXE_ERROR_RATE, sp.EXE_ERROR_RATE)),4)
                        tr_time_cost = round(func['data_size'] / accessible_server["t_rate"],4)
                        self.edge_servers[accessible_server["server_id"]].initial_new_container(request['user_id'],request['task_name'],request['instance_name'],
                                                                                            container_id,ex_time_cost,tr_time_cost,request['es_time_cost'])
                        request["status"] = 'allocated'
                        break
            
            if request["status"] != 'allocated':
                self.users[request['user_id']].not_allocated(request['task_name'],request['instance_name'])
               
        return True
    
    def new_user(self,id) :
        return True
        self.users.append(ef.User(self.env,len(self.users)))

