#! /usr/bin/env python

'''
Created on 01/07/2014

Principal module of CKPTMAN

@author: Amanda
'''

# imports de librerias de python
import time
import threading
import subprocess
import logging
import os.path 
import pyslurm

# imports de ficheros propios
from im_connector import * 
from config import * # aqui estan definidas las variables
import hour 
import threshold 

# Otra forma de crear el logging
#configure logger
#logger = logging.getLogger('ckptman')
#hdlr = logging.FileHandler(LOG_FILE)
#formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
#hdlr.setFormatter(formatter)
#logger.addHandler(hdlr)
#logger.setLevel(logging.DEBUG)

# Metodo que inicializa el logging de ckptman
def init():
	logging.basicConfig(filename=LOG_FILE, 
						level=logging.DEBUG, 
						format='%(asctime)s: %(levelname)-8s %(message)s',
						datefmt='%m-%d-%Y %H:%M:%S',
						filemode='w')
						
	logging.info('************ Starting ckptman daemon **************')

# Metodo encargado de obtener la lista de trabajo en ejecucion en SLURM (via pyslurm)
def get_job_info():
	a = pyslurm.job()
	jobs = a.get()
	job_list = {}

	if jobs:
	   for key, value in jobs.iteritems():
			state = str(value.get("job_state")[1])
			if state == 'RUNNING':
				job_list [str(key)] = str(value.get("nodes"))
				
	return job_list

# Metodo encargado de comprobar si el trabajo SLURM ha acabado correctamente
def is_job_completed(job_id):
	# /tmp/terminated_jobs contains the jobs that had completed the epilog phase, so they finished successfully
	if os.path.exists("/home/ubuntu/terminated_jobs"): #/tmp/terminated_jobs
		f = open("/home/ubuntu/terminated_jobs", "r")  #/tmp/terminated_jobs
		for line in f:
			if line[-1] == '\n':
				line = line[:-1]
			if line == job_id:
				#logging.debug("Job" + job_id + " terminated successfully.")
				return True
		#logging.warning("Job" + job_id + " terminated abruptly!")
		f.close()
	else: 
		logging.warning("terminated_jobs file does not exist.")
		return False
	
# Metodo encargado de comprobar si existen ficheros de checkpoint para el trabajo
def check_ckpt_file(job_id):
	if os.path.exists("/home/ubuntu/" + str(job_id)): 
		return True 
	else: 
		return False

# Metodo para obtener el comando que se empleo para lanzar el trabajo, con el objetivo de relanzarlo de nuevo
def obtain_sbatch_command(job_id):
	a = pyslurm.job()
	jobs = a.get()
	command = ""

	if jobs:
	   for key, value in jobs.iteritems():
			if key == job_id:
				command = str(value.get("command"))
	logging.info("Command for job " + job_id + " is: " + command)
	return command
				
	
# Metodo encargado de actualizar el diccionario de pares (nodo_spot:trabajo)
def refresh_dictionary():
	# dictionary that will store (spot node:job) pairs
	dic = {}
	
	# Llamar al IM para que me diga el tipo de nodos mediante im_connector.py
	# nodes tiene el formato: {'front': 'ondemand', 'wnode9': 'spot', 'wnode8': 'spot'}
	logging.debug('Ask to the IM the name and type of nodes of the infrastructure.')
	nodes = getInfrastructureInfo()
	
	# Recorremos el diccionario de nodos en busca de los nodos spot que no tengamos apuntados en nuestro diccionario
	if len(nodes) > 0:
		for key, value in nodes.iteritems():
			if value == 'spot' and key not in dic:
				dic[key] = ""
				logging.debug("Adding node " + key + " to the dictionary")
			else:
				logging.debug("Skipping to add node " + key + " to the dictionary. It is not spot or it's already in the dictionary.")
	else: 
		logging.warning("There are not nodes in the infrastructure.")

	# Obtener lista de trabajos slurm
	# jobs tiene el formato: {'4': 'wnode8'}
	jobs = get_job_info()
	logging.info("The running job list in the infrastructure is: ") 
	logging.info(jobs)
	
	# Guardar en el diccionario trabajos que esten en ejecucion en spot instances
	if len(jobs) > 0:
		for key, value in jobs.iteritems():
			if value in dic:
				dic[value] = key #guardamos el trabajo en el nodo que esta ejecutandose
	else: 
		logging.warning("There are not jobs running the infrastructure.")
	
	# Devolver el diccionario
	return dic

# Este es el metodo que controla el checkpointing y el estado de los nodos (si siguen vivos o han caido)
def checkpoint_control(dic):
	if len(dic) > 0:
		logging.info("We have nodes to control.")
		# primero habra que ver si tenia trabajo asociado (value != "")
		for key, value in dic.iteritems():
			if value != "":
				logging.debug("Node " + key + " has jobs executing.")
				# luego habra que ver si el nodo esta vivo mediante el IM
				# estados de las VMs: unknown, pending, running, stopped, off, failed, configured
				state = str(get_node_state(key))
				logging.info("State of node " + key + " is " + state)
				if state != "running" and state != "configured":
					logging.warning("Node " + key + " is dead.")
					# Nodo muerto. Ver si acabo su ejecucion (epilog de SLURM) y si no lo hizo hay que volver a encolarlo
					completed = is_job_completed(value)
					if completed:
						logging.debug("Job " + value + " terminated successfully. No more actions required.")
					else:
						logging.warning("Job " + value + " terminated abruptly!")
						#comprobar si existe un fichero de checkpoint
						ckptFile = check_ckpt_file(value)
						#si es asi: scontrol checkpoint restart <job_id>
						if ckptFile:
							logging.debug("Checkpoint file exists. Time to restart a job from a checkpoint.")
							try:
								run_command("scontrol checkpoint restart " + value)
								logging.debug("Success restarting the job from the checkpointing file.")
							except CommandError:
								logging.error("Command failed while restarting the job from the checkpointing file because SLURM do not know that the node is dead.")
							# Esperamos a que SLURM detecte la caida del nodo
							time.sleep(60)
							
							try:
								run_command("scontrol checkpoint restart " + value)
								logging.debug("Success restarting the job from the checkpointing file.")
							except CommandError:
								logging.error("Command failed while restarting the job from the checkpointing file.")
							except DownNodeError:
								logging.debug("Success restarting the job from the checkpointing file, regardless the error.")
						else:
							#si no existe, reiniciar el trabajo reencolandolo en la cola
							#Se puede hacer o bien mediante scontrol requeue <job_id> o automaticamente activando a 1 el atributo JobRequeue de slurm.conf
							#Parece que si desactivo el jobrequeue no me deja hacerlo a mano a mi tampoco: slurm_requeue error: Requested operation is presently disabled
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
					# si esta vivo y ejecutando habra que aplicarle el algoritmo de checkpoint (que sera el que determine
					# si tiene que hacer o no checkpoint (que devuelvan un booleano)) que haya elegido el usuario
					if CKPT_ALGORITHM == 'HOUR':
						logging.info("Using HOUR Checkpointing algorithm.")
						# Obtener el tiempo en que se lanzo la VM (llamando al IM)
						launch_time = get_launch_time(key)
						logging.info("Launch time of node " + key + " is " + str(launch_time))
						if launch_time:
							# Llamar al modulo que contiene el algoritmo de checkpoint HOUR
							ckpt = hour.is_checkpoint_time(launch_time, key)
							# entonces hacer el checkpoint (con run_command) si es el momento adecuado
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
						# Obtener el tiempo en que se lanzo la VM (llamando al IM)
						launch_time = get_launch_time(key)
						logging.info("Launch time of node " + key + " is " + str(launch_time))
						if launch_time:
							# Llamar al modulo que contiene el algoritmo de checkpoint THRESHOLD
							ckpt = threshold.is_checkpoint_time(launch_time, key)
							# entonces hacer el checkpoint (con run_command) si es el momento adecuado
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

def launch_daemon():
	# A lo mejor aqui interesa hacer un while true y dejarse de timers
	while True:
		nodes_jobs_dic = refresh_dictionary()
		checkpoint_control(nodes_jobs_dic)
		time.sleep(REVALUE_TIME)
	#timer = threading.Timer(30.0, ckptman_daemon)
	#timer.start() 
	
#############################################################
#				Utiles de Carlos y de la web				#
#############################################################

# Clase y metodo para ejecutar un comando bash
	
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
        if err == "scontrol_checkpoint error: Required node not available (down or drained)\n":
            raise DownNodeError()
        else:
            raise CommandError()
    else:
        return output

# Metodo para ejecutar algo periodicamente
def hello():
	logging.debug('hello, world!')
	t = threading.Timer(2.0, hello)
	t.start() # after 2 seconds, "hello, world" will be printed
	
# Clase y metodo para crear un hilo
class ClaseQueHaceAlgo(threading.Thread):
    def run(self):
        while True:
            print("ejecucion de comando en el thread secundario " + run_command("/bin/hostname"))
            time.sleep(1)

			
# metodo main
if __name__ == '__main__':
	init()
	launch_daemon()

	#c = ClaseQueHaceAlgo()
	#c.daemon = True
	#c.start()
	
	#while True:
		#print("hago algo en el thread principal")
		#time.sleep(1)
		#timer = threading.Timer(30.0, hello)
		
	# Esto ejecuta el metodo refresh_dictionary tras 30 segundos de ponerse en marcha y acaba, no lo ejecuta periodicamente
	#timer = threading.Timer(30.0, refresh_dictionary)
	#timer.start() 
	
	# Para ejecutar algo periodicamente, tengo que hacer el metodo recursivo (http://stackoverflow.com/questions/8600161/executing-periodic-actions-in-python)
	# hello() # ejemplo de ejecucion periodica	
