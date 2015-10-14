#! /usr/bin/env python

'''
Created on 01/07/2014

Principal module of CKPTMAN

ckptman - Checkpointing Manager for BLCR and spot instances
2014 - GRyCAP - Universitat Politecnica de Valencia
@author: Amanda
'''

import time
import threading
import subprocess
import logging
import os.path

from im_connector import * 
from config import * 
import hour 
import threshold 

# Iniciates the ckptman logging
def init():
	logging.basicConfig(filename=LOG_FILE, 
						level=logging.DEBUG, 
						format='%(asctime)s: %(levelname)-8s %(message)s',
						datefmt='%m-%d-%Y %H:%M:%S',
						filemode='w')
						
	logging.info('************ Starting ckptman daemon **************')

def parse_scontrol(out):
	if out.find("=") < 0: return []
 	r = []
	for line in out.split("\n"):
		line = line.strip()
		if not line: continue
		d = {}; r.append(d); s = False
		for k in [ j for i in line.split("=") for j in i.rsplit(" ", 1) ]:
			if s: d[f] = k
			else: f = k
			s = not s
	return r

# Obtains the list of jobs that are in execution in SLURM
def get_job_info():
	exit = parse_scontrol(run_command("scontrol -o show jobs -a"))
	job_list = {}
	if exit:
		for key in exit:
			if key["JobState"] == "RUNNING" or key["JobState"] == "NODE_FAIL":
				job_list [str(key["JobId"])] = str(key["BatchHost"])
	return job_list

# Checks if the SLURM job has finished correctly
def is_job_completed(job_id):
	# /tmp/terminated_jobs contains the jobs that had completed the epilog phase, so they finished successfully
	if os.path.exists("/home/ubuntu/terminated_jobs"): 
		f = open("/home/ubuntu/terminated_jobs", "r")
		for line in f:
			if line[-1] == '\n':
				line = line[:-1]
			if line == job_id:
				return True
		f.close()
	else: 
		logging.warning("terminated_jobs file does not exist.")
		return False
	
# Checks if there are some checkpoint files for the job
def check_ckpt_file(job_id):
	if os.path.exists("/home/ubuntu/" + str(job_id)): 
		return True 
	else: 
		return False

# Obtains the command used to launch the job, in order to relaunch it again
def obtain_sbatch_command(job_id):
	jobs = parse_scontrol(run_command("scontrol -o show jobs -a"))
	command = ""
	if jobs:
		for key in jobs:
			if key["JobId"] == str(job_id):
				command = str(key["Command"])
	logging.info("Command for job " + job_id + " is: " + command)
	return command
	
# Refresh the dictionary of pairs (spot_node:job)
def refresh_dictionary():
	# dictionary that will store (spot node:job) pairs
	dic = {}
	
	# Call the IM to obtain the nodes type
	# nodes will be like: {'front': 'ondemand', 'wnode9': 'spot', 'wnode8': 'spot'}
	logging.debug('Ask to the IM the name and type of nodes of the infrastructure.')
	nodes = getInfrastructureInfo()
	
	# Search for new spot nodes
	if len(nodes) > 0:
		for key, value in nodes.iteritems():
			if value == 'spot' and key not in dic:
				dic[key] = ""
				logging.debug("Adding node " + key + " to the dictionary")
			else:
				logging.debug("Skipping to add node " + key + " to the dictionary. It is not spot or it's already in the dictionary.")
	else: 
		logging.warning("There are not nodes in the infrastructure.")

	# Obtain the slurm jobs list
	# jobs will be like: {'4': 'wnode8'}
	jobs = get_job_info()
	logging.info("The running job list in the infrastructure is: ") 
	logging.info(jobs)
	
	# Save into the dictionary the jobs that are being executed in a spot node
	if len(jobs) > 0:
		for key, value in jobs.iteritems():
			if value in dic:
				dic[value] = key
	else: 
		logging.warning("There are not jobs running the infrastructure.")
	
	return dic

# Controls the state of the nodes and perform checkpointing actions if proceed
def checkpoint_control(dic):
	if len(dic) > 0:
		logging.info("We have nodes to control.")
		for key, value in dic.iteritems():
			if value != "":
				logging.debug("Node " + key + " has jobs executing.")
				# VM state can be: unknown, pending, running, stopped, off, failed, configured
				state = str(get_node_state(key))
				logging.info("State of node " + key + " is " + state)
				if state != "running" and state != "configured":
					logging.warning("Node " + key + " is dead.")
					# Dead node. Check if it finishes its execution
					completed = is_job_completed(value)
					if completed:
						logging.debug("Job " + value + " terminated successfully. No more actions required.")
					else:
						logging.warning("Job " + value + " terminated abruptly!")
						# Check if there is a checkpoint file
						ckptFile = check_ckpt_file(value)
						# If yes: scontrol checkpoint restart <job_id>
						if ckptFile:
							logging.debug("Checkpoint file exists. Time to restart a job from a checkpoint.")
							try:
								run_command("scontrol checkpoint restart " + value)
								logging.debug("Success restarting the job from the checkpointing file.")
							except CommandError:
								logging.error("Command failed while restarting the job from the checkpointing file because SLURM do not know that the node is dead.")
								
							# Wait for SLURM detects the dead node
							time.sleep(100)

							try:
								run_command("scontrol checkpoint restart " + value)
								logging.debug("Success restarting the job from the checkpointing file.")
							except CommandError:
								logging.error("Command failed while restarting the job from the checkpointing file.")
							except DownNodeError:
								logging.debug("Success restarting the job from the checkpointing file, regardless the error.")
						else:
							# If there is no checkpoint file, restart the job from the beginning
							logging.warning("Checkpoint file DO NOT exist. SLURM will Restart the job from the beginning.")
							command = obtain_sbatch_command(value)
							if command != "":
								run_command("sbatch " + command)
								#run_command("scontrol requeue " + value)
								logging.debug("Success requeuing the job from the beginning")
							else:
								logging.error("Command of job " + value + " is none")
				else:
					logging.info("Node " + key + " is alive.")
					# The node is alive. Apply checkpointing algorithm
					if CKPT_ALGORITHM == 'HOUR':
						logging.info("Using HOUR Checkpointing algorithm.")
						launch_time = get_launch_time(key)
						logging.info("Launch time of node " + key + " is " + str(launch_time))
						if launch_time:
							ckpt = hour.is_checkpoint_time(launch_time, key)
							if ckpt:
								logging.debug("Time to perform a checkpoint.")
								run_command("scontrol checkpoint create " + value)
								logging.debug("Checkpointing performed successfully.")
							else:
								logging.debug("It's NOT time to perform a checkpoint.")
						else:
							logging.error("Error obtaining launch time of node " + key)
					elif CKPT_ALGORITHM == 'THRESHOLD':
						logging.info("Using THRESHOLD Checkpointing algorithm.")
						launch_time = get_launch_time(key)
						logging.info("Launch time of node " + key + " is " + str(launch_time))
						if launch_time:
							ckpt = threshold.is_checkpoint_time(launch_time, key)
							if ckpt:
								logging.debug("Time to perform a checkpoint.")
								run_command("scontrol checkpoint create " + value)
								logging.debug("Checkpointing performed successfully.")
							else:
								logging.debug("It's NOT time to perform a checkpoint.")
						else:
							logging.error("Error obtaining launch time of node " + key)
					else:
						logging.error("The specified checkpointing algorithm is not recognized.")
			else:
				logging.debug("The node " + key + " has no jobs executing. No more actions required.")
	else:
		logging.warning("The nodes_job dictionary is empty!")

# Launch ckptman daemon
def launch_daemon():
	while True:
		nodes_jobs_dic = refresh_dictionary()
		checkpoint_control(nodes_jobs_dic)
		time.sleep(REVALUE_TIME)

	
#############################################################
#	 Class and methods to execute bash commands         #
#############################################################
	
class CommandError(Exception):pass

class DownNodeError(Exception):pass

def run_command(command, shell=False):
    string = " "
    try:
        logging.debug("executing: %s" % command)
        p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    except:
        if type(command)==list: command = string.join(command)
        logging.error('Could not execute command "%s"' %command)
        raise
    
    (output, err) = p.communicate()
    if p.returncode != 0:
        if type(command)==list: command = string.join(command)
        logging.error(' Error in command "%s"' % command)
        logging.error(' Return code was: %s' % p.returncode)
        logging.error(' Error output was:\n%s' % err)
        if err == "scontrol_checkpoint error: Required node not available (down, drained or reserved)\n":
            raise DownNodeError()
        else:
            raise CommandError()
    else:
        return output
			
# main method
if __name__ == '__main__':
	init()
	launch_daemon()
	
