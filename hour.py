'''
Created on 01/07/2014

HOURly Checkpointing algorithm: checkpoints are taken just prior 
to the beginning of next instance hour. Since Amazon is not charging
any partial hour, this scheme will save as much tasks as the user 
is paying.

ckptman - Checkpointing Manager for BLCR and spot instances
2014 - GRyCAP - Universitat Politecnica de Valencia
 
@author: Amanda
'''

import time
import logging
import datetime

from config import * 

HOUR_DURATION = 3600

# launch_time is a Linux timestamp like: 1403174380
def is_checkpoint_time(launch_time, hostname):
	checkpoint = False
	launched = datetime.datetime.fromtimestamp(int(launch_time)).strftime('%Y-%m-%d %H:%M:%S')  
	logging.info("HOUR: Time node " + hostname + " was launched is " + launched)
	# Obtain actual hour
	now = time.strftime("%H:%M:%S")       
	logging.info("HOUR: Actual time is " + now) 
	actual_time = int(time.time())
	
	# Calculate the time the node is running
	live_time = actual_time - launch_time
	
	# Check if it's time to make a checkpoint
	remaining_hour_time = HOUR_DURATION - live_time % HOUR_DURATION
	logging.debug("HOUR: Remaining hour time = %d for node %s" % (int(remaining_hour_time), hostname))
	if int(remaining_hour_time) < int(CKPT_TIME_MARGIN):
		checkpoint = True
			
	return checkpoint

	
