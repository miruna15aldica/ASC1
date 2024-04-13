from queue import Queue
from threading import Thread, Event
import time
from app.data_ingestor import DataIngestor
import os
import json
import logging
from logging.handlers import RotatingFileHandler
from threading import Lock
class Job:
    def __init__(self, id_no, command, question, state=None):
        self.id_no = id_no # NUmarul id-ului
        self.command = command # Comanda data (ex best5, worst5)
        self.question = question # Intrebarea pusa
        self.state = state # Statul pe care se aplica intrebarea, daca este cazul
        self.status = 'running' # Statusul (running sau done)

      
class ThreadPool:
    def __init__(self):
        self.max_threads = self.get_max_no_threads() # Numarul maxim de threaduri
        self.shutdown = False # Shutdown
        self.threads = [] # Threadurile in sine
        self.taskQ = Queue() # Coada de taskuri, elem de sincronizarea
        self.running_jobs = dict() # Joburile in desfasurare 
        self.running_jobs_lock = Lock() # Lock pentru joburile in desfasurare
        self.finish_jobs = dict() # Joburile terminate
        self.finish_jobs_lock = Lock() # Lock pentru joburile terminate
        for _ in range(self.max_threads):
            task_runner = TaskRunner(self)
            self.threads.append(task_runner) 
            task_runner.start() # Start

    def get_max_no_threads(self): # Functie pentru aflarea numarului de threaduri
        env_threads = os.getenv('TP_NUM_OF_THREADS')
        if env_threads is not None:
            return int(env_threads)
        return os.cpu_count()
        
    def shutdown(self):
        for i in self.threads:
            self.taskQ.put(None) # Adaugam None in coada

        for i in self.threads:
            self.threads[i].join() # Asteptam terminarea fiecarui thread

class TaskRunner(Thread):
    def __init__(self, task_runner):
        Thread.__init__(self)
        self.task_runner = task_runner
        self.data = DataIngestor('./nutrition_activity_obesity_usa_subset.csv').data # Baza de date pentru care facem calculele

    def run(self):
        while True:
            job = self.task_runner.taskQ.get() # Luam un job din coada
            if job is None: # Daca jobul e Null
                break
            output_p = os.getcwd() + "/results/" + f"{job.id_no}.json" # Calea/fisierul in care scriem rezultatul
            with self.task_runner.running_jobs_lock:
                self.task_runner.running_jobs[job.id_no] = 'running' # Jobul este in starea de running
            command = job.command # Tratam fiecare comanda in parte
            if command == "best5": # implementat
                result = self.best5(self.data, job.question)
            elif command == "global_mean": # implementat
                result = self.global_mean(self.data, job.question)
                
            elif command == "diff_from_mean": # implementat
                result = self.diff_from_mean(self.data, job.question)

            elif command == "mean_by_category": # TODO - ia 9/10p
                result = self.mean_by_category(self.data, job.question)

            elif command == "state_diff_from_mean": # implementat
                result = self.state_diff_from_mean(self.data, job.question, job.state)
            
            elif command == "state_mean": # implementat
                result = self.state_mean(self.data, job.question, job.state)

            elif command == "state_mean_by_category": # implementat
                result = self.state_mean_by_category(self.data, job.question, job.state)

            elif command == "states_mean": # implementat
                result = self.states_mean(self.data, job.question)

            elif command == "worst5": # implementat
                result = self.worst5(self.data, job.question)

            else:
                result = self.handle_invalid_command() # Daca comanda nu e valida

            with open(output_p, 'w+') as f:
                f.write(json.dumps(result)) # Scriem rezultatul comenzii in fisierul de output
            with self.task_runner.finish_jobs_lock: 
                self.task_runner.finish_jobs[job.id_no] = 'finished' # Am scris rezultatul, deci jobul este terminat
  
    # Comanda pentru requestul /api/states_mean
    def states_mean(self, data: dict, question: str):
        elem = {} 
        sum = {}
        datas = [i for i in data if i['Question'] == question] # Filtram datele dupa intrebarea primita
        for i in datas: 
            elem[i['LocationDesc']] = 1 + elem.get(i['LocationDesc'], 0) # Numarul de elemente pentru un anumit stat
            sum[i['LocationDesc']] = float(i['Data_Value']) + sum.get(i['LocationDesc'], 0) # Suma pentru fiecare stat in parte
        states_mean = {i: sum[i] / elem[i] for i in sum} #  Media
        return dict(sorted(states_mean.items(), key = lambda x : x[1])) # Sortare crescatoare dupa medie

    # Comanda pentru requestul /api/state_mean
    def state_mean(self, data: dict, question: str, state: str):
        sum = 0
        datas = [i for i in data if i['LocationDesc'] == state and  i['Question'] == question]
        # Filtram datele dupa intrebarea si statul primite ca parametru
        elem = len(datas) # NUmarul de inregistrari valabile
        for i in datas:
            sum += float(i['Data_Value'])
        mean = sum / elem # Media artimetica
        return {state : mean} 

    # Comanda pentru requestul /api/best5
    def best5(self, data: dict, question: str):
        questions_best_is_min = [
            'Percent of adults aged 18 years and older who have an overweight classification',
            'Percent of adults aged 18 years and older who have obesity',
            'Percent of adults who engage in no leisure-time physical activity',
            'Percent of adults who report consuming fruit less than one time daily',
            'Percent of adults who report consuming vegetables less than one time daily'
        ] # Intrebarule pentru care best e valoarea minima

        questions_best_is_max = [
            'Percent of adults who achieve at least 150 minutes a week of moderate-intensity aerobic physical activity or 75 minutes a week of vigorous-intensity aerobic activity (or an equivalent combination)',
            'Percent of adults who achieve at least 150 minutes a week of moderate-intensity aerobic physical activity or 75 minutes a week of vigorous-intensity aerobic physical activity and engage in muscle-strengthening activities on 2 or more days a week',
            'Percent of adults who achieve at least 300 minutes a week of moderate-intensity aerobic physical activity or 150 minutes a week of vigorous-intensity aerobic activity (or an equivalent combination)',
            'Percent of adults who engage in muscle-strengthening activities on 2 or more days a week',
        ] # Intrebarile pentru care best e valoarea maxima

        elem = {} 
        sum = {}
        data = [i for i in data if i['Question'] == question] # Sortam datele dupa intrebarea cautata
        for i in data: 
            state = i['LocationDesc']
            elem[state] = 1 + elem.get(state, 0) # Numarul de elemente pentru un stat 
            sum[state] =  float(i['Data_Value']) + sum.get(state, 0) # Suma valorilor pentru un stat
        states_mean = {i: sum[i] / elem[i] for i in sum}
        if question in questions_best_is_min:
            return dict(sorted(states_mean.items(), key = lambda x : x[1])[:5]) # Sortare
        elif question in questions_best_is_max:
            return dict(list(reversed(sorted(states_mean.items(), key = lambda x : x[1])[-5:]))) # Sortare

    # Comanda pentru requestul /api/worst5
    def worst5(self, data: dict, question: str):
        questions_best_is_min = [
            'Percent of adults aged 18 years and older who have an overweight classification',
            'Percent of adults aged 18 years and older who have obesity',
            'Percent of adults who engage in no leisure-time physical activity',
            'Percent of adults who report consuming fruit less than one time daily',
            'Percent of adults who report consuming vegetables less than one time daily'
        ] # Intrebarule pentru care best e valoarea minima

        questions_best_is_max = [
            'Percent of adults who achieve at least 150 minutes a week of moderate-intensity aerobic physical activity or 75 minutes a week of vigorous-intensity aerobic activity (or an equivalent combination)',
            'Percent of adults who achieve at least 150 minutes a week of moderate-intensity aerobic physical activity or 75 minutes a week of vigorous-intensity aerobic physical activity and engage in muscle-strengthening activities on 2 or more days a week',
            'Percent of adults who achieve at least 300 minutes a week of moderate-intensity aerobic physical activity or 150 minutes a week of vigorous-intensity aerobic activity (or an equivalent combination)',
            'Percent of adults who engage in muscle-strengthening activities on 2 or more days a week',
        ] # Intrebarile pentru care best e valoarea maxima
        elem = {} 
        sum = {}
        data = [i for i in data if i['Question'] == question] # Sortam datele dupa intrebarea cautata
        for i in data: 
            state = i['LocationDesc']
            elem[state] = 1 + elem.get(state, 0) # Numarul de elemente pentru un stat 
            sum[state] =  float(i['Data_Value']) + sum.get(state, 0) # Suma valorilor pentru un stat
        states_mean = {i: sum[i] / elem[i] for i in sum}
        if question in questions_best_is_min:
            return dict(sorted(states_mean.items(), key = lambda x : x[1])[-5:])
        elif question in questions_best_is_max:
            return dict(list(reversed(sorted(states_mean.items(), key = lambda x : x[1])[:5])))

    # Comanda pentru requestul /api/global_mean
    def global_mean(self, data: dict, question: str):
        elem = 0
        sum = 0
        datas = [i for i in data if i['Question'] == question]  # Sortam datele dupa intrebarea cautata
        for i in datas:
            sum += float(i['Data_Value'])
            elem += 1
        return {"global_mean": float(sum / elem)} # media aritmetica

    # Comanda pentru requestul /api/diff_from_mean
    def diff_from_mean(self, data: list, question: str):
        elem_no = 0
        sum1 = 0
        elem = {}
        sum = {}    
        datas = [i for i in data if i['Question'] == question] # Sortam datele dupa intrebarea cautata
        for i in datas: 
            state = i['LocationDesc']
            elem[state] = 1 + elem.get(state, 0) # Numarul de elemente pentru un stat anume
            sum[state] =  float(i['Data_Value']) + sum.get(state, 0) # Suma valorilor pentru un stat anume
            sum1 += float(i['Data_Value'])
            elem_no += 1
        mean = float(sum1 / elem_no) # Media globala pentru o intrebare selectata
        states_mean = {i: sum[i] / elem[i] for i in sum}       
        return {j: mean - i for j, i in sorted(states_mean.items(), key = lambda x : x[1])}

    # Comanda pentru requestul /api/state_diff_from_mean
    def state_diff_from_mean(self, data:dict, question: str, state: str):
        elem_no = 0
        sum1 = 0
        state_sum = 0  
        elem_state = 0
        datas = [i for i in data if i['Question'] == question] # Sortam datele dupa intrebarea cautata
        for i in datas: 
            sum1 += float(i['Data_Value'])
            elem_no += 1
            if i['LocationDesc'] == state: # Daca in datele respective gasim statul cautat
                state_sum += float(i['Data_Value'])
                elem_state += 1
        return {state: float(sum1 / elem_no) - state_sum / elem_state}

    # Comanda pentru requestul /api/mean_by_category
    def mean_by_category(self, data: list, question: str):
        elem = {}
        mean = {}
        datas = [i for i in data if i['Question'] == question]
        for i in datas:
            per = str((i['LocationDesc'], i['StratificationCategory1'], i['Stratification1'])) # Construim tuplu pe baza inf
            if not per in mean.keys(): # Daca nu tuplul deja se gaseste in cheile noastre, initializam
                elem[per] = 1
                mean[per] = float(i['Data_Value'])
            else: 
                mean[per] += float(i['Data_Value'])
                elem[per] += 1
        for i in mean:
            mean[i] = mean[i] / float(elem[i])
        return mean 
    
    # Comanda pentru requestul /api/state_mean_by_category
    def state_mean_by_category(self, data: list, question: str, state: str):
        elem = {}
        mean = {}
        datas = [i for i in data if i['LocationDesc'] == state and  i['Question'] == question] # Construim tuplul pe baza inf
        for i in datas:
            a = str((i['StratificationCategory1'], i['Stratification1']))
            if a not in mean.keys(): # Daca tuplul nu se gaseste in cheile noastre, initializam
                elem[str(a)] = 1
                mean[str(a)] = float(i['Data_Value'])        
            else: # Daca se gaseste in cheie
                elem[str(a)] += 1
                mean[str(a)] += float(i['Data_Value'])
        for i in mean:
            mean[i] = mean[i] / float(elem[i])
        result = {state: mean}
        return result
    
    # Comanda pentru un request invalid
    def handle_invalid_command(self):
        return {"status" : "Invalid command"}
    

    

                    



                    

                        



