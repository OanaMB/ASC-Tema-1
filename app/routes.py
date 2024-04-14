import os
import json
from flask import request, jsonify
from app import webserver

# Example endpoint definition
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

    # Method Not Allowed
    return jsonify({"error": "Method not allowed"}), 405

@webserver.route('/api/get_results/<job_id>', methods=['GET'])
def get_response(job_id):
    # Check if job_id is valid
    job_number = job_id.split("_")[2]
    if int(job_number) > webserver.job_counter:
        return jsonify({"status": "running","reason": "Invalid job_id"})

    # Check if job_id is done and return the result
    result_file = f"results/{job_id}.json"
    if os.path.exists(result_file):
        try:
            with open(result_file, "r", encoding="utf-8") as file:
                result = json.loads(file.read())
            file.close()
            return jsonify({'status': 'done', 'data': result})
        except json.decoder.JSONDecodeError:
            return jsonify({"status": "running", "reason": "Error reading result file"})

    # Check if job_id is done and return the result
    #    res = res_for(job_id)
    #    return jsonify({
    #        'status': 'done',
    #        'data': res
    #    })

    # If not, return running status
    return jsonify({'status': 'running'})

@webserver.route('/api/graceful_shutdown', methods=['GET'])
def graceful_shutdown():
    # Set the graceful_shutdown_event
    webserver.tasks_runner.graceful_shutdown()
    return jsonify({"message": "Shutting down gracefully"})

@webserver.route('/api/num_jobs', methods=['GET'])
def num_jobs():
    # Get the number of jobs in the queue and the number of running jobs
    number_jobs = webserver.tasks_runner.jobs.qsize() + len(webserver.tasks_runner.running_jobs)
    return jsonify({"status": "done", "data": number_jobs})

@webserver.route('/api/jobs', methods=['GET'])
def jobs():
    # Get the status of all jobs
    jobs_available = {}
    for i in range(1,webserver.job_counter + 1):
        # Check if the job is done
        if os.path.exists(f"results/job_id_{i}.json"):
            jobs_available[f"job_id_{i}"] = "done"
        # Check if the job is running
        elif f"job_id_{i}" in webserver.tasks_runner.running_jobs:
            jobs_available[f"job_id_{i}"] = "running"
        # Check if the job is pending
        else:
            jobs_available[f"job_id_{i}"] = "pending"
    return jsonify({"status": "done", "data":jobs_available})

@webserver.route('/api/states_mean', methods=['POST'])
def states_mean_request():
    # Check if the server is shutting down
    if webserver.tasks_runner.graceful_shutdown_event.is_set():
        return jsonify({"status": "error", "reason": "Shutting down"})

    # Get request data
    data = request.json
    question = data.get('question')

    # Register job. Don't wait for task to finish
    # Increment job_id counter
    # Return associated job_id
    job_id = "job_id_" + str(webserver.job_counter)
    job = (job_id, webserver.data_ingestor.states_mean, [question])
    webserver.tasks_runner.jobs.put(job)
    webserver.job_counter += 1

    return jsonify({"job_id": job_id})

@webserver.route('/api/state_mean', methods=['POST'])
def state_mean_request():
    # Check if the server is shutting down
    if webserver.tasks_runner.graceful_shutdown_event.is_set():
        return jsonify({"status": "error", "reason": "Shutting down"})

    # Get request data
    # Register job. Don't wait for task to finish
    # Increment job_id counter
    # Return associated job_id
    data = request.json
    question = data.get('question')
    state = data.get('state')

    job_id = "job_id_" + str(webserver.job_counter)
    job = (job_id, webserver.data_ingestor.state_mean, [question, state])
    webserver.tasks_runner.jobs.put(job)
    webserver.job_counter += 1

    return jsonify({"job_id": job_id})


@webserver.route('/api/best5', methods=['POST'])
def best5_request():
    # Check if the server is shutting down
    if webserver.tasks_runner.graceful_shutdown_event.is_set():
        return jsonify({"status": "error", "reason": "Shutting down"})

    # Get request data
    # Register job. Don't wait for task to finish
    # Increment job_id counter
    # Return associated job_id
    data = request.json
    question = data.get('question')
    job_id_x = "job_id_" + str(webserver.job_counter)
    job_id = (job_id_x, webserver.data_ingestor.best5, [question])
    webserver.tasks_runner.jobs.put(job_id)
    webserver.job_counter += 1

    return jsonify({"job_id": job_id_x})

@webserver.route('/api/worst5', methods=['POST'])
def worst5_request():
    # Check if the server is shutting down
    if webserver.tasks_runner.graceful_shutdown_event.is_set():
        return jsonify({"status": "error", "reason": "Shutting down"})

    # Get request data
    # Register job. Don't wait for task to finish
    # Increment job_id counter
    # Return associated job_id
    data = request.json
    question = data.get('question')
    job_id = "job_id_" + str(webserver.job_counter)
    job = (job_id, webserver.data_ingestor.worst5, [question])
    webserver.tasks_runner.jobs.put(job)
    webserver.job_counter += 1

    return jsonify({"job_id": job_id})

@webserver.route('/api/global_mean', methods=['POST'])
def global_mean_request():
    # Check if the server is shutting down
    if webserver.tasks_runner.graceful_shutdown_event.is_set():
        return jsonify({"status": "error", "reason": "Shutting down"})

    # Get request data
    # Register job. Don't wait for task to finish
    # Increment job_id counter
    # Return associated job_id
    data = request.json
    question = data.get('question')
    job_id = "job_id_" + str(webserver.job_counter)
    job = (job_id, webserver.data_ingestor.global_mean, [question])
    webserver.tasks_runner.jobs.put(job)
    webserver.job_counter += 1

    return jsonify({"job_id": job_id})

@webserver.route('/api/diff_from_mean', methods=['POST'])
def diff_from_mean_request():
    # Check if the server is shutting down
    if webserver.tasks_runner.graceful_shutdown_event.is_set():
        return jsonify({"status": "error", "reason": "Shutting down"})

    # Get request data
    # Register job. Don't wait for task to finish
    # Increment job_id counter
    # Return associated job_id
    data = request.json
    question = data.get('question')
    job_id = "job_id_" + str(webserver.job_counter)
    job = (job_id, webserver.data_ingestor.diff_from_mean, [question])
    webserver.tasks_runner.jobs.put(job)
    webserver.job_counter += 1

    return jsonify({"job_id": job_id})

@webserver.route('/api/state_diff_from_mean', methods=['POST'])
def state_diff_from_mean_request():
    # Check if the server is shutting down
    if webserver.tasks_runner.graceful_shutdown_event.is_set():
        return jsonify({"status": "error", "reason": "Shutting down"})

    # Get request data
    # Register job. Don't wait for task to finish
    # Increment job_id counter
    # Return associated job_id
    data = request.json
    question = data.get('question')
    state = data.get('state')
    job_id = "job_id_" + str(webserver.job_counter)
    job = (job_id, webserver.data_ingestor.state_diff_from_mean, [question,state])
    webserver.tasks_runner.jobs.put(job)
    webserver.job_counter += 1

    return jsonify({"job_id": job_id})

@webserver.route('/api/mean_by_category', methods=['POST'])
def mean_by_category_request():
    # Check if the server is shutting down
    if webserver.tasks_runner.graceful_shutdown_event.is_set():
        return jsonify({"status": "error", "reason": "Shutting down"})

    # Get request data
    # Register job. Don't wait for task to finish
    # Increment job_id counter
    # Return associated job_id
    data = request.json
    question = data.get('question')
    job_id = "job_id_" + str(webserver.job_counter)
    job = (job_id, webserver.data_ingestor.mean_by_category, [question])
    webserver.tasks_runner.jobs.put(job)
    webserver.job_counter += 1

    return jsonify({"job_id": job_id})

@webserver.route('/api/state_mean_by_category', methods=['POST'])
def state_mean_by_category_request():
    # Check if the server is shutting down
    if webserver.tasks_runner.graceful_shutdown_event.is_set():
        return jsonify({"status": "error", "reason": "Shutting down"})

    # Get request data
    # Register job. Don't wait for task to finish
    # Increment job_id counter
    # Return associated job_id
    data = request.get_json()
    question = data['question']
    state = data['state']
    job_id = "job_id_" + str(webserver.job_counter)
    job = (job_id, webserver.data_ingestor.state_mean_by_category, [question,state])
    webserver.tasks_runner.jobs.put(job)
    webserver.job_counter += 1

    return jsonify({"job_id": job_id})

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
