'''
Created on 16/07/2014

Threshold Checkpointing algorithm: checkpoints are taken when a rising in the 
price of the spot instance is observed inside the interval. The interval is
a % of the price the user has determined. 

ckptman - Checkpointing Manager for BLCR and spot instances
2014 - GRyCAP - Universitat Politecnica de Valencia
@author: Amanda
'''
import boto.ec2
import logging
import time
import datetime
import os
import calendar
#from iso8601 import parse_date
from im_connector import * 
from config import * 
from spot_mock import spot_mock

HOUR_DURATION = 3600
TEST_INIT_TIME = int(time.time())
spot_price = spot_mock()

# list that contains the last historic price of the spot instances
historical_price = [0,0]

# Converts the amazon timestamp (ISO 8601) into unix format
#def iso2unix(timestamp):
    # use iso8601.parse_date to convert the timestamp into a datetime object.
    #parsed = parse_date(timestamp)
    # now grab a time tuple that we can feed mktime
    #timetuple = parsed.timetuple()

    #return calendar.timegm(timetuple)

    
# Determine if it's time to perform a checkpoint
def is_checkpoint_time(launch_time, hostname):
    checkpoint = False
    
    #############################################################################
    # First we check if an hour has passed away since the creation of the node  #
    #############################################################################
    
    launched = datetime.datetime.fromtimestamp(int(launch_time)).strftime('%Y-%m-%d %H:%M:%S')  
    logging.info("THRESHOLD: Time node " + hostname + " was launched is " + launched)
    # Obtain actual time
    now = time.strftime("%H:%M:%S")    
    logging.info("THRESHOLD: Actual time is " + now) 
    actual_time = int(time.time())
    live_time = actual_time - launch_time
    
    # Check if it's time to make a checkpoint
    remaining_hour_time = HOUR_DURATION - live_time % HOUR_DURATION
    logging.debug("THRESHOLD: Remaining hour time = %d for node %s" % (int(remaining_hour_time), hostname))
    if int(remaining_hour_time) < int(CKPT_TIME_MARGIN):
        checkpoint = True
        #return checkpoint
    
    #############################################################################
    # Second we check if there are recent variations in the spot price market   #
    #############################################################################

    region = get_region(hostname)
    logging.info("THRESHOLD: The region used to connect with AWS is " + region)

    instance_type = get_instance_type(hostname)
    logging.info("THRESHOLD: The instance type to check the spot prices is " + instance_type) 
    
    availability_zone= get_availability_zone(hostname)
    logging.info("THRESHOLD: The availability zone to check the spot prices is " + availability_zone) 

    auth_data = get_credentials()
    if auth_data:
        for auth in auth_data:
            if auth["type"]=='EC2':
                access_key = auth["username"]
                secret_key= auth["password"]
    else:
        logging.error("THRESHOLD: Error obtaining user credentials for EC2")
    
    bid = get_user_spot_bid(hostname)
    logging.info("THRESHOLD: The user's bid is " + str(bid))

    # Calculate the interval values
    #res = bid * THRESHOLD/100
    #limit = bid - res

        
    os.environ['AWS_ACCESS_KEY_ID'] = access_key
    os.environ['AWS_SECRET_ACCESS_KEY'] = secret_key
    
    # Create the EC2 connection
    #ec2 = boto.ec2.connect_to_region(region)
        
    if not checkpoint:
        
        # Calculate start time and end time for the boto request
        #end_time = datetime.datetime.now().isoformat()
        #epoch_time = int(time.time())
        #start_time = datetime.datetime.strptime((epoch_time - 600), "%Y-%m-%dT%H:%M:%S.%fZ")
        #start_time = datetime.datetime.fromtimestamp(int(epoch_time - 600)).strftime('%Y-%m-%d %H:%M:%S')  
        
        #history = ec2.get_spot_price_history(start_time=start_time, end_time=end_time,instance_type=instance_type, availability_zone=availability_zone)

        end_time = int(time.time()) - TEST_INIT_TIME
        start_time = end_time - 600 
        history = spot_price.get_spot_price_history(start=start_time, end=end_time)
        
        #average = history[0].price
        sum = 0
        if history:
            for h in history:
                sum = sum + h.price
            average = sum/len(history)
            limit = (average + bid)/2
            logging.debug("THRESHOLD: The limit (threshold) value is: " + str(limit))
            logging.info("LIMIT: " + str(limit))
            logging.debug("THRESHOLD: Current spot price for the availability zone " + availability_zone + " : " + str(history[0].price) + " at " + str(history[0].timestamp))
            logging.info("PRICE: " + str(history[0].price))
            logging.info("TIME: " + str(history[0].timestamp))
            logging.info("ACTUAL_TIME: " + str(int(time.time()) - TEST_INIT_TIME))
            if(historical_price[0] != 0 and historical_price[1] != 0):
                #if historical_price[0] != str(iso2unix(history[0].timestamp)):
                if historical_price[0] != history[0].timestamp:
                    if history[0].price > float(historical_price[1]):
                        if history[0].price > limit:
                            logging.info("THRESHOLD: A variation in the spot price inside the interval has been detected")
                            checkpoint = True

            #historical_price[0] = iso2unix(history[0].timestamp)
            historical_price[0] = history[0].timestamp
            historical_price[1] = history[0].price
        else:
            logging.error("THRESHOLD: Cannot get the current spot price from Amazon")
    else:
        end_time = int(time.time()) - TEST_INIT_TIME
        history = spot_price.get_spot_price_history(start=0, end=end_time)
        #historical_price[0] = iso2unix(history[0].timestamp)
        historical_price[0] = history[0].timestamp
        historical_price[1] = history[0].price

    return checkpoint

