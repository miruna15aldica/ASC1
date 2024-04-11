from queue import Queue
from threading import Thread, Event
import time
from app.data_ingestor import DataIngestor
import os
import json

 
class Job:
    def __init__(self, id_no, command, question, state=None):
        self.id_no = id_no # aici trebuie sa ma folosesc cumva de job_id ul meu, cred
        self.command = command
        # self.data = data
        self.question = question
        self.state = state
        self.status = 'running'


      
class ThreadPool:
    taskQ = Queue()
    running_jobs = dict()
    finish_jobs = dict()
    def __init__(self):
        self.max_threads = self.get_max_no_threads() # numarul de threaduri
        # self.taskQ = Queue() # coada de taskuri, aceasta este in sine un element de sincronizare
        self.threads = []
        for _ in range(self.max_threads):
            task_runner = TaskRunner()
            self.threads.append(task_runner)
            task_runner.start()
            
        # TODO - dupa ce i-am dat start, cred ca ar fi momentul sa ii dam si join
        # if ThreadPool.taskQ.empty(): # abia cand nu mai avem taskuri in coada de asteptare
        #     for i in self.threads:
        #         i.join()
    # def get_task_queue(self):
    #     if self.taskQ.empty():
    #         return None
    #     return self.taskQ.get()

    def get_max_no_threads(self):
        env_threads = os.getenv('TP_NUM_OF_THREADS')
        if env_threads is not None:
            return int(env_threads)
        return os.cpu_count() # numarul maxim de threaduri

class TaskRunner(Thread):
    def __init__(self):
        # TODO: init necessary data structures
        Thread.__init__(self) # initializam threadurile
        self.data = DataIngestor('./nutrition_activity_obesity_usa_subset.csv').data # baza de date
        # self.taskQ = taskQ
        # self.thread_pool = thread_pool



    def run(self):
        while True:
            # TODO
            job = ThreadPool.taskQ.get()
            if job is not None:
                output_p = os.getcwd() + "/results/" + f"{job.id_no}.json"
                output_path = os.makedirs(os.path.dirname(output_p), exist_ok=True)

                # job.status = 'running'
                ThreadPool.running_jobs[job.id_no] = 'running'
                command = job.command
                if command == "best5": # implementat
                    result = self.best5(self.data, job.question)
                elif command == "global_mean": # implementat
                    result = self.global_mean(self.data, job.question)
                    
                elif command == "diff_from_mean": # implementat
                    result = self.diff_from_mean(self.data, job.question)
                    # result = self.best5(self.data, job.question)

                elif command == "mean_by_category": # TODO - ia 9/10p
                    result = self.mean_by_category(self.data, job.question)

                elif command == "state_diff_from_mean": # implementat
                    result = self.state_diff_from_mean(self.data, job.question, job.state)
                
                elif command == "state_mean": # implementat
                    result = self.state_mean(self.data, job.question, job.state)

                elif command == "state_mean_by_category": # implementat, DAR MODIFICA
                    result = self.state_mean_by_category(self.data, job.question, job.state)

                elif command == "states_mean": # implementat
                    result = self.states_mean(self.data, job.question)

                elif command == "worst5": # implementat
                    result = self.worst5(self.data, job.question)

                else:
                    result = self.handle_invalid_command()

                with open(output_p, 'w+') as f:
                    f.write(json.dumps(result))
                ThreadPool.finish_jobs[job.id_no] = 'finished'
            else:
                if ThreadPool.taskQ.empty(): # abia cand nu mai avem taskuri in coada de asteptare
                    for i in self.threads:
                        i.join()
                


    def states_mean(self, data: dict, question: str):
        #Primește o întrebare (din setul de întrebări de mai sus) și calculează media valorilor
        # înregistrate (Data_Value) din intervalul total de timp (2011 - 2022) pentru fiecare stat,
        # și sortează crescător după medie.
        results = {}
        sum = {}
        counts = {} 
        filtered = [i for i in data if i['Question'] == question] # valorile filtrate pentru fiecare intrare a dictionarului
        for i in filtered: # mergem prin toate valorile ce corespund intrebarii
            state = i['LocationDesc'] # stat
            value = float(i['Data_Value']) # valoarea corespunzatoare statului
            counts[state] = counts.get(state, 0) + 1
            sum[state] = sum.get(state, 0) + value
        states_mean = {state: sum[state] / counts[state] for state in sum}
        sorted_result = sorted(states_mean.items(), key = lambda x : x[1])
        sorted_results_dict = dict(sorted_result) # am facut cast la dictionar sa mi le afiseze altfel
        return sorted_results_dict

    
    def state_mean(self, data: dict, question: str, state: str):
        value = 0
        filtered = [i for i in data if i['Question'] == question and i['LocationDesc'] == state]
        for i in filtered:
            value += float(i['Data_Value'])
        mean_value = value / len(filtered)
        state_mean_dict = {state : mean_value}  # Crează un dicționar cu cheia 'state' și valoarea 'mean_value'
        return state_mean_dict


    def best5(self, data: dict, question: str):
        results = {}
        sum = {}
        counts = {} 
        filtered = [i for i in data if i['Question'] == question] # valorile filtrate pentru fiecare intrare a dictionarului
        for i in filtered: # mergem prin toate valorile ce corespund intrebarii
            state = i['LocationDesc'] # stat
            value = float(i['Data_Value']) # valoarea corespunzatoare statului
            counts[state] = counts.get(state, 0) + 1
            sum[state] = sum.get(state, 0) + value
        states_mean = {state: sum[state] / counts[state] for state in sum}
        sorted_result = sorted(states_mean.items(), key = lambda x : x[1])
        sorted_results_dict = dict(sorted_result) # am facut cast la dictionar sa mi le afiseze altfel
        questions_best_is_min = [
            'Percent of adults aged 18 years and older who have an overweight classification',
            'Percent of adults aged 18 years and older who have obesity',
            'Percent of adults who engage in no leisure-time physical activity',
            'Percent of adults who report consuming fruit less than one time daily',
            'Percent of adults who report consuming vegetables less than one time daily'
        ]

        questions_best_is_max = [
            'Percent of adults who achieve at least 150 minutes a week of moderate-intensity aerobic physical activity or 75 minutes a week of vigorous-intensity aerobic activity (or an equivalent combination)',
            'Percent of adults who achieve at least 150 minutes a week of moderate-intensity aerobic physical activity or 75 minutes a week of vigorous-intensity aerobic physical activity and engage in muscle-strengthening activities on 2 or more days a week',
            'Percent of adults who achieve at least 300 minutes a week of moderate-intensity aerobic physical activity or 150 minutes a week of vigorous-intensity aerobic activity (or an equivalent combination)',
            'Percent of adults who engage in muscle-strengthening activities on 2 or more days a week',
        ]
        if question in questions_best_is_min:
            best_states = dict(sorted_result[:5])
        elif questions_best_is_max:
            best_states = list(reversed(sorted_result[-5:]))
            best_states = dict(best_states)
        else:
            return "Invalid question"
        return best_states


    def worst5(self, data: dict, question: str):
        results = {}
        sum = {}
        counts = {} 
        filtered = [i for i in data if i['Question'] == question] # valorile filtrate pentru fiecare intrare a dictionarului
        for i in filtered: # mergem prin toate valorile ce corespund intrebarii
            state = i['LocationDesc'] # stat
            value = float(i['Data_Value']) # valoarea corespunzatoare statului
            counts[state] = counts.get(state, 0) + 1
            sum[state] = sum.get(state, 0) + value
        states_mean = {state: sum[state] / counts[state] for state in sum}
        sorted_result = sorted(states_mean.items(), key = lambda x : x[1])
        sorted_results_dict = dict(sorted_result) # am facut cast la dictionar sa mi le afiseze altfel
        questions_best_is_min = [
            'Percent of adults aged 18 years and older who have an overweight classification',
            'Percent of adults aged 18 years and older who have obesity',
            'Percent of adults who engage in no leisure-time physical activity',
            'Percent of adults who report consuming fruit less than one time daily',
            'Percent of adults who report consuming vegetables less than one time daily'
        ]

        questions_best_is_max = [
            'Percent of adults who achieve at least 150 minutes a week of moderate-intensity aerobic physical activity or 75 minutes a week of vigorous-intensity aerobic activity (or an equivalent combination)',
            'Percent of adults who achieve at least 150 minutes a week of moderate-intensity aerobic physical activity or 75 minutes a week of vigorous-intensity aerobic physical activity and engage in muscle-strengthening activities on 2 or more days a week',
            'Percent of adults who achieve at least 300 minutes a week of moderate-intensity aerobic physical activity or 150 minutes a week of vigorous-intensity aerobic activity (or an equivalent combination)',
            'Percent of adults who engage in muscle-strengthening activities on 2 or more days a week',
        ]
        if question in questions_best_is_max:
            worst = dict(sorted_result[:5])
        elif question in questions_best_is_min:
            # worst = list(reversed(sorted_result[-5:]))
            # worst = dict(worst)
            worst = dict(sorted_result[-5:])
        else:
            return "Invalid question"
        return worst

    def global_mean(self, data: dict, question: str):
        results = {}
        sum = 0
        counts = 0
        mean = 0
        filtered = [i for i in data if i['Question'] == question] # valorile filtrate pentru fiecare intrare a dictionarului
        for i in filtered: # mergem prin toate valorile ce corespund intrebarii
            sum += float(i['Data_Value'])
            counts += 1

        mean = float(sum / counts)
        return {"global_mean": mean}

    def diff_from_mean(self, data: list, question: str):

        sum1 = 0
        counts1 = 0
        mean = 0
        filtered = [i for i in data if i['Question'] == question] # valorile filtrate pentru fiecare intrare a dictionarului
        for i in filtered: # mergem prin toate valorile ce corespund intrebarii
            sum1 += float(i['Data_Value'])
            counts1 += 1

        mean = float(sum1 / counts1)
        results = {}
        sum = {}
        counts = {} 
        filtered = [i for i in data if i['Question'] == question] # valorile filtrate pentru fiecare intrare a dictionarului
        for i in filtered: # mergem prin toate valorile ce corespund intrebarii
            state = i['LocationDesc'] # stat
            value = float(i['Data_Value']) # valoarea corespunzatoare statului
            counts[state] = counts.get(state, 0) + 1
            sum[state] = sum.get(state, 0) + value
        states_mean = {state: sum[state] / counts[state] for state in sum}
        sorted_result = sorted(states_mean.items(), key = lambda x : x[1])
        
        return {key: mean - value for key, value in sorted_result}



    def state_diff_from_mean(self, data:dict, question: str, state: str):
        # Cred ca nu e ok pentru faptul ca apeleaza late functii, cred ca se duce intr un loop un thread pe undeva
        sum1 = 0
        counts1 = 0
        mean = 0
        filtered = [i for i in data if i['Question'] == question] # valorile filtrate pentru fiecare intrare a dictionarului
        for i in filtered: # mergem prin toate valorile ce corespund intrebarii
            sum1 += float(i['Data_Value'])
            counts1 += 1

        mean = float(sum1 / counts1)
        value = 0
        filtered = [i for i in data if i['Question'] == question and i['LocationDesc'] == state]
        for i in filtered:
            value += float(i['Data_Value'])
        mean_value = value / len(filtered)
        dictionar = {state: mean - mean_value}
        return dictionar

    def mean_by_category(self, data: list, question: str):
        mean = {}
        intrari = {}
        filtered = [i for i in data if i['Question'] == question]
        for i in filtered:
            perechi = str((i['LocationDesc'], i['StratificationCategory1'], i['Stratification1']))
            if perechi in mean.keys():

                mean[perechi] += float(i['Data_Value'])
                intrari[perechi] += 1
            else:
                intrari[perechi] = 1
                mean[perechi] = float(i['Data_Value'])
        for i in mean:
            mean[i] = mean[i] / float(intrari[i])
        # results = dict(results)
        return mean 
    

    def state_mean_by_category(self, data: list, question: str, state: str):
    # Result dictionary
        state_mean = {}
        num_of_entries = {}
        for data_line in data:
            if state == data_line['LocationDesc']:
                if question == data_line['Question']:
                    a = str((data_line['StratificationCategory1'], data_line['Stratification1']))
                    if a in state_mean.keys():
                        num_of_entries[str(a)] += 1
                        state_mean[str(a)] += float(data_line['Data_Value'])
                        
                    else:
                        num_of_entries[str(a)] = 1 # o luam de la capat
                        state_mean[str(a)] = float(data_line['Data_Value'])
                        

        for i in state_mean:
            state_mean[i] = state_mean[i] / float(num_of_entries[i])
        return {state : state_mean}
    
    def handle_invalid_command(self):
        return {"status" : "Invalid command"}
    

    

                    



                    

                        



