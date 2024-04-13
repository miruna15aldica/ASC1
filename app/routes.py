from app import webserver
from flask import request, jsonify
from app.task_runner import Job, ThreadPool, TaskRunner
import os
import json
from threading import Lock

job_idd = 0 # Job id-ul de care ne folosim
job_idd_lock = Lock() # Lock pentru job_id, elem de sincronizare
@webserver.route('/api/post_endpoint', methods=['POST'])
def post_endpoint():
    if request.method == 'POST':
        data = request.json
        response = {"message": "Received data successfully", "data": data}
        return jsonify(response)
    else:
        return jsonify({"error": "Method not allowed"}), 405

@webserver.route('/api/get_results/<job_id>', methods=['GET'])
def get_response(job_id):
    with webserver.task_runner.finish_jobs_lock:
        # Verificam daca job_id-ul este al unul job in desfasurare sau terminat
        if int(job_id.split("_")[2]) in webserver.task_runner.finish_jobs:
            output_p = os.getcwd() + "/results/" + f"{job_id.split('_')[2]}.json"
            if os.path.getsize(output_p) > 0: # Am verificat ca fisierul nu e gol
                with open(output_p, 'r') as f:
                    res = json.load(f)
                return jsonify({
                    'status': 'done',
                    'data': res
                }) # Returnam rezultatul
            return jsonify({
                'status': 'Invalid Job id',
                'data': None
            })
        
    with webserver.task_runner.running_jobs_lock:
        if int(job_id.split("_")[2]) in webserver.task_runner.running_jobs:
            return jsonify({'status': 'running'})
    
    return jsonify({
        'status': 'Invalid Job id',
        'data': None
    })

@webserver.route('/api/states_mean', methods=['POST'])
def states_mean_request():
    data = request.json
    with job_idd_lock: # Pentru sincronizare, vrem un singur thread sa acceseze resursa la un moment de timp
        global job_idd
        job_idd += 1
        new_job = Job(id_no=job_idd, command='states_mean', question=data['question']) # Noul job
    webserver.task_runner.taskQ.put(new_job) # Punem noul job in coada de taskuri
    return jsonify({'job_id' : f"job_id_{job_idd}"})

@webserver.route('/api/state_mean', methods=['POST'])
def state_mean_request():
    data = request.json
    with job_idd_lock: # Pentru sincronizare, vrem un singur thread sa acceseze resursa la un moment de timp
        global job_idd
        job_idd += 1
        new_job = Job(id_no=job_idd, command='state_mean',
                    question=data['question'], state=data['state']) # Noul job
    webserver.task_runner.taskQ.put(new_job)
    return jsonify({'job_id' : f"job_id_{job_idd}"})

@webserver.route('/api/best5', methods=['POST'])
def best5_request():
    data = request.json
    with job_idd_lock: # Pentru sincronizare, vrem un singur thread sa acceseze resursa la un moment de timp
        global job_idd
        job_idd += 1
        new_job = Job(id_no=job_idd, command='best5', question=data['question']) # Noul job
    webserver.task_runner.taskQ.put(new_job)
    return jsonify({'job_id' : f"job_id_{job_idd}"})

@webserver.route('/api/worst5', methods=['POST'])
def worst5_request():
    data = request.json
    with job_idd_lock: # Pentru sincronizare, vrem un singur thread sa acceseze resursa la un moment de timp
        global job_idd
        job_idd += 1
        new_job = Job(id_no=job_idd, command='worst5', question=data['question']) # Noul job
    webserver.task_runner.taskQ.put(new_job)
    return jsonify({'job_id' : f"job_id_{job_idd}"})

@webserver.route('/api/graceful_shutdown', methods=['GET'])
def graceful_shutdown():
    webserver.task_runner.shutdown()
    
@webserver.route('/api/global_mean', methods=['POST'])
def global_mean_request():
    data = request.json
    with job_idd_lock: # Pentru sincronizare, vrem un singur thread sa acceseze resursa la un moment de timp
        global job_idd
        job_idd += 1
        new_job = Job(id_no=job_idd, command='global_mean', question=data['question']) # Noul job
    webserver.task_runner.taskQ.put(new_job)
    return jsonify({'job_id' : f"job_id_{job_idd}"})

@webserver.route('/api/diff_from_mean', methods=['POST'])
def diff_from_mean_request():
    data = request.json
    with job_idd_lock: # Pentru sincronizare, vrem un singur thread sa acceseze resursa la un moment de timp
        global job_idd
        job_idd += 1
        new_job = Job(id_no=job_idd, command='diff_from_mean', question=data['question']) # Noul job
    webserver.task_runner.taskQ.put(new_job)
    return jsonify({'job_id' : f"job_id_{job_idd}"})

@webserver.route('/api/state_diff_from_mean', methods=['POST'])
def state_diff_from_mean_request():
    data = request.json
    with job_idd_lock: # Pentru sincronizare, vrem un singur thread sa acceseze resursa la un moment de timp
        global job_idd
        job_idd += 1
        new_job = Job(id_no=job_idd, command='state_diff_from_mean', 
                    question=data['question'], state=data['state']) # Noul job
    webserver.task_runner.taskQ.put(new_job)
    return jsonify({'job_id' : f"job_id_{job_idd}"})

@webserver.route('/api/mean_by_category', methods=['POST'])
def mean_by_category_request():
    data = request.json
    with job_idd_lock: # Pentru sincronizare, vrem un singur thread sa acceseze resursa la un moment de timp
        global job_idd
        job_idd += 1
        new_job = Job(id_no=job_idd, command='mean_by_category', question=data['question']) # Noul job
    webserver.task_runner.taskQ.put(new_job)
    return jsonify({'job_id' : f"job_id_{job_idd}"})

@webserver.route('/api/state_mean_by_category', methods=['POST'])
def state_mean_by_category_request():
    data = request.json
    with job_idd_lock: # Pentru sincronizare, vrem un singur thread sa acceseze resursa la un moment de timp
        global job_idd
        job_idd += 1
        new_job = Job(id_no=job_idd, command='state_mean_by_category', question=data['question'], state=data['state']) # Noul job
    webserver.task_runner.taskQ.put(new_job)
    return jsonify({'job_id' : f"job_id_{job_idd}"})

@webserver.route('/api/jobs', methods=['GET'])
def jobs():
    jobs_inf = []
    for i in webserver.task_runner.running_jobs: # Daca jobul cu id-ul i are task in continua desfasurare
        jobs_inf.append({i : "running"})
    for i in webserver.task_runner.finish_jobs: # Daca jobul s-a incheiat
        jobs_inf.append({i: "done"})

    return jsonify({'status': 'done', 'data': jobs_inf})

@webserver.route('/api/num_jobs', methods=['GET'])
def num_jobs():
    running_jobs = len(webserver.task_runner.running_jobs) # Numarul de joburi in desfasurare
    return jsonify({'status' : 'done', 'data': running_jobs})

@webserver.route('/')
@webserver.route('/index')
def index():
    routes = get_defined_routes()
    msg = f"Hello, World!\n Interact with the webserver using one of the defined routes:\n"
    paragraphs = ""
    for route in routes:
        paragraphs += f"<p>{route}</p>"

    msg += paragraphs
    return msg

def get_defined_routes():
    routes = []
    for rule in webserver.url_map.iter_rules():
        methods = ', '.join(rule.methods)
        routes.append(f"Endpoint: \"{rule}\" Methods: \"{methods}\"")
    return routes
