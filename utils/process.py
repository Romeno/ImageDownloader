import os
import time
import logging
import queue
import subprocess, threading
import multiprocessing as mp


class Command(object):
    def __init__(self, cmd):
        self.cmd = cmd
        self.process = None

    def run(self, timeout=None, **kwargs):
        def target(kwargs):
            self.process = subprocess.Popen(self.cmd, shell=False, **kwargs)

            self.process.communicate()
        thread = threading.Thread(target=target, args=(kwargs,))
        thread.start()

        thread.join(timeout=timeout)
        if thread.is_alive():
            self.process.terminate()
            thread.join()
            raise ProcessTimeoutError(self.process.returncode)

        return self.process.returncode

# end of Command


class ProcessTimeoutError(Exception):
    def __init__(self, retcode):
        super(ProcessTimeoutError, self).__init__()
        self.retcode = retcode

# end of ProcessTimeoutError


class ProcessPool(object):
    """
    The class represents pool of processes that do some jobs
    The count of processes is determined by len() of data constructor argument
    Supports Ctrl-C. When hit stops all the child processes with KeyboardInterrupt,
    waits for them and finishes.
    """

    def __init__(self, poolLength, worker, data):
        self.poolLength = poolLength
        self.worker = worker
        self.data = data
        self.logger_name = "errors.log"

    # end of __init__

    def JobDispatcher(self, job_queue, result_queue):
        logger = logging.getLogger(self.logger_name)
        # logger = Logger.GetLogger(Logger.Type.Fw)
        logger.info("Process started: %d" % os.getpid())
        try:
            while not job_queue.empty():
                logger.info("Getting new job")
                job = job_queue.get(block=False)
                logger.info("Job started: %s" % str(job) )
                result_queue.put(self.worker(job))
                logger.info("Job finished: %s" % str(job) )
        except queue.Empty:
            logger.warning("QUEUE EMPTY")
            pass

        logger.info( "Exiting: %s" % str(os.getpid()) )

    # end of JobDispatcher

    def Run(self):
        logger = logging.getLogger(self.logger_name)
        # logger = Logger.GetLogger(Logger.Type.Fw)

        job_queue = mp.Queue()
        result_queue = mp.Queue()

        for data in self.data:
            job_queue.put(data)

        workers = []
        for _ in range(self.poolLength):
            tmp = mp.Process(target=self.JobDispatcher,
                             args=(job_queue, result_queue))
            tmp.start()
            workers.append(tmp)

        logger.info("----------------------------------Started.")
        try:
            for worker in workers:
                logger.info("----------------------------------Joining process %s" % worker.pid)
                worker.join()
        except KeyboardInterrupt:
            while True:
                time.sleep(1)
                logger.info("----------------------------------Waiting for child processes to finish outputting the results...")

                aliveCount = 0
                for worker in workers:
                    if worker.is_alive():
                        aliveCount += 1

                if aliveCount:
                    logger.info("----------------------------------Still %d alive..." % aliveCount)
                else:
                    logger.info("----------------------------------Done!")
                    break

            return result_queue

        return result_queue

    # end of Run

# end of ProcessPool


class SilentProcessPool(ProcessPool):
    def JobDispatcher(self, job_queue, result_queue):

        super(SilentProcessPool, self).JobDispatcher(job_queue, result_queue)

    # end of JobDispatcher

# end of SilentProcessPool
 







