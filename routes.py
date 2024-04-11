from app import webserver
from flask import request, jsonify
from app.task_runner import Job, ThreadPool, TaskRunner
import os
import json

job_idd = 0
# TODO - cum pot sa scap de acest job id? vreau sa fac ceva cu el
# Example endpoint definition
# TODO - inlocuieste if True acela cu altceva
@webserver.route('/api/post_endpoint', methods=['POST'])
def post_endpoint():
    if request.method == 'POST':
        # Assuming the request contains JSON data
        data = request.json
        print(f"got data in post {data}")

        # Process the received data
        # For demonstration purposes, just echoing back the received data
        response = {"message": "Received data successfully", "data": data}
        # Sending back a JSON response
        return jsonify(response)
    else:
        # Method Not Allowed
        return jsonify({"error": "Method not allowed"}), 405

@webserver.route('/api/get_results/<job_id>', methods=['GET'])
def get_response(job_id):
    print(f"JobID is {job_id}")
    print("Uite si varianta cu join")
    # TODO
    # Check if job_id is valid
    if int(job_id.split("_")[2]) in ThreadPool.finish_jobs:
        # print(int(job_id.split("_")[2]))
        output_p = os.getcwd() + "/results/" + f"{job_id.split('_')[2]}.json"
        # if output_p.exists():
        if os.path.getsize(output_p) > 0:
            with open(output_p, 'r') as f:
                res = json.load(f)
                print(job_id)
                print(res)
            return jsonify({
                'status': 'done',
                'data': res
            })
        elif int(job_id.split("_")[2]) in ThreadPool.running_jobs:
            return jsonify({'status': 'running'})


    # If not, return running status
    return jsonify({
        'status': 'Invalid Job id',
        'data': None
    })


@webserver.route('/api/states_mean', methods=['POST'])
def states_mean_request():
    # TODO
    data = request.json
    global job_idd
    job_idd += 1 # incrementam job id-ul
    new_job = Job(id_no=job_idd, command='states_mean', question=data['question'])
    ThreadPool.taskQ.put(new_job)
    # TODO = fa un job si pune-l in threadpool
    return jsonify({'job_id' : f"job_id_{job_idd}"}) # returnam job id-ul asociat

@webserver.route('/api/state_mean', methods=['POST'])
def state_mean_request():
    # TODO
    data = request.json
    global job_idd
    job_idd += 1 # incrementam job id-ul
    new_job = Job(id_no=job_idd, command='state_mean', question=data['question'], state=data['state'])
    ThreadPool.taskQ.put(new_job)
    # TODO = fa un job si pune-l in threadpool
    return jsonify({'job_id' : f"job_id_{job_idd}"}) # returnam job id-ul asociat



@webserver.route('/api/best5', methods=['POST'])
def best5_request():
    # TODO
    data = request.json
    global job_idd
    job_idd += 1 # incrementam job id-ul
    new_job = Job(id_no=job_idd, command='best5', question=data['question'])
    ThreadPool.taskQ.put(new_job)
    # TODO = fa un job si pune-l in threadpool
    return jsonify({'job_id' : f"job_id_{job_idd}"}) # returnam job id-ul asociat

@webserver.route('/api/worst5', methods=['POST'])
def worst5_request():
    # TODO
    data = request.json
    global job_idd
    job_idd += 1 # incrementam job id-ul
    new_job = Job(id_no=job_idd, command='worst5', question=data['question'])
    ThreadPool.taskQ.put(new_job)
    # TODO = fa un job si pune-l in threadpool
    return jsonify({'job_id' : f"job_id_{job_idd}"}) # returnam job id-ul asociat

@webserver.route('/api/global_mean', methods=['POST'])
def global_mean_request():
    # TODO
    data = request.json
    global job_idd
    job_idd += 1 # incrementam job id-ul
    new_job = Job(id_no=job_idd, command='global_mean', question=data['question'])
    ThreadPool.taskQ.put(new_job)
    # TODO = fa un job si pune-l in threadpool
    return jsonify({'job_id' : f"job_id_{job_idd}"}) # returnam job id-ul asociat

@webserver.route('/api/diff_from_mean', methods=['POST'])
def diff_from_mean_request():
    data = request.json
    global job_idd
    job_idd += 1 # incrementam job id-ul
    new_job = Job(id_no=job_idd, command='diff_from_mean', question=data['question'])
    ThreadPool.taskQ.put(new_job)
    # TODO = fa un job si pune-l in threadpool
    return jsonify({'job_id' : f"job_id_{job_idd}"}) # returnam job id-ul asociat

@webserver.route('/api/state_diff_from_mean', methods=['POST'])
def state_diff_from_mean_request():
    # TODO
    data = request.json
    global job_idd
    job_idd += 1 # incrementam job id-ul
    new_job = Job(id_no=job_idd, command='state_diff_from_mean', question=data['question'], state=data['state'])
    ThreadPool.taskQ.put(new_job)
    # TODO = fa un job si pune-l in threadpool
    return jsonify({'job_id' : f"job_id_{job_idd}"}) # returnam job id-ul asociat

@webserver.route('/api/mean_by_category', methods=['POST'])
def mean_by_category_request():
    # TODO
    data = request.json
    global job_idd
    job_idd += 1 # incrementam job id-ul
    new_job = Job(id_no=job_idd, command='mean_by_category', question=data['question'])
    ThreadPool.taskQ.put(new_job)
    # TODO = fa un job si pune-l in threadpool
    return jsonify({'job_id' : f"job_id_{job_idd}"}) # returnam job id-ul asociat

@webserver.route('/api/state_mean_by_category', methods=['POST'])
def state_mean_by_category_request():
    # TODO
    data = request.json
    global job_idd
    job_idd += 1 # incrementam job id-ul
    new_job = Job(id_no=job_idd, command='state_mean_by_category', question=data['question'], state=data['state'])
    ThreadPool.taskQ.put(new_job)
    # TODO = fa un job si pune-l in threadpool
    return jsonify({'job_id' : f"job_id_{job_idd}"}) # returnam job id-ul asociat

# You can check localhost in your browser to see what this displays
@webserver.route('/')
@webserver.route('/index')
def index():
    routes = get_defined_routes()
    msg = f"Hello, World!\n Interact with the webserver using one of the defined routes:\n"

    # Display each route as a separate HTML <p> tag
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
