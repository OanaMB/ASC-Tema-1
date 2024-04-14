from queue import Queue
from threading import Thread, Event, Lock
import os
import json
import multiprocessing

class ThreadPool:
    def __init__(self):
        # You must implement a ThreadPool of TaskRunners
        # Your ThreadPool should check if an environment variable TP_NUM_OF_THREADS is defined
        # If the env var is defined, that is the number of threads to be used by the thread pool
        # Otherwise, you are to use what the hardware concurrency allows
        # You are free to write your implementation as you see fit, but
        # You must NOT:
        #   * create more threads than the hardware concurrency allows
        #   * recreate threads for each task
        self.num_threads = self._get_num_threads()
        self.jobs = Queue()
        self.running_jobs = []
        self.task_runners = []
        self.graceful_shutdown_event = Event()
        self.lock = Lock()
        self.start_threads()

    def _get_num_threads(self):
        num_threads_env = os.getenv('TP_NUM_OF_THREADS')
        if num_threads_env is not None:
            return int(num_threads_env)

        return multiprocessing.cpu_count()

    def start_threads(self):
        for _ in range(self.num_threads):
            task_runner = TaskRunner(self.jobs, self.running_jobs, self.graceful_shutdown_event, self.lock)
            task_runner.start()
            self.task_runners.append(task_runner)

    def graceful_shutdown(self):
        self.graceful_shutdown_event.set()
        # Add None to the queue to signal the threads to stop
        for task_runner in self.task_runners:
            self.jobs.put(None)
        for task_runner in self.task_runners:
            task_runner.join()

class TaskRunner(Thread):
    def __init__(self, jobs_queue, running_jobs_queue, graceful_shutdown_event, lock):
        # init necessary data structures
        Thread.__init__(self)
        self.jobs_queue = jobs_queue
        self.running_jobs_queue = running_jobs_queue
        self.graceful_shutdown_event = graceful_shutdown_event
        self.lock = lock

    def run(self):
        while True:
            # Get pending job
            # Execute the job and save the result to disk
            # Repeat until graceful_shutdown
            # end the thread if graceful_shutdown
            if self.graceful_shutdown_event.is_set():
                break

            # Get the next job from the queue
            job = self.jobs_queue.get()
            if job is None:
                break

            # Unpack the job
            job_id, func, args = job
            # Add job_id to the running jobs queue
            self.running_jobs_queue.append(job_id)
            # Execute the task associated with job_id
            result = func(*args)
            # Save the result to disk
            filename = f"results/{job_id}.json"
            with open(filename, "w", encoding="utf-8") as file:
                json.dump(result, file, ensure_ascii=False)
            file.close()
            # Remove job_id from the running jobs queue
            with self.lock:
                self.running_jobs_queue.remove(job_id)
