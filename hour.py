'''
Created on 01/07/2014

HOURly Checkpointing algorithm: checkpoints are taken just prior 
to the beginning of next instance hour. Since Amazon is not charging
any partial hour, this scheme will save as much tasks as the user 
is paying.

@author: Amanda
'''

import time
import logging
import datetime

from config import * # aqui estan definidas las variables

HOUR_DURATION = 3600

# launch_time es un timestamp de linux formato: 1403174380
def is_checkpoint_time(launch_time, hostname):
	checkpoint = False
	# Transformamos el timestamp en el formato manejado por python - NO hace falta
	launched = datetime.datetime.fromtimestamp(int(launch_time)).strftime('%Y-%m-%d %H:%M:%S')  
	logging.info("HOUR: Time node " + hostname + " was launched is " + launched)
	# Obtenemos la hora actual
	now = time.strftime("%H:%M:%S") # En formato string          
	logging.info("HOUR: Actual time is " + now) 
	actual_time = int(time.time())
	
	# Calculamos el tiempo que lleva el nodo en marcha
	live_time = actual_time - launch_time
	
	# Comprobamos si hay que hacer checkpoint
	remaining_hour_time = HOUR_DURATION - live_time % HOUR_DURATION
	logging.debug("HOUR: Remaining hour time = %d for node %s" % (int(remaining_hour_time), hostname))
	if int(remaining_hour_time) < int(CKPT_TIME_MARGIN):
		checkpoint = True
			
	return checkpoint

	
