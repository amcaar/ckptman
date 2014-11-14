'''
Created on 16/07/2014
Threshold Checkpointing algorithm: checkpoints are taken when a rising in the 
price of the spot instance is observed inside the interval. The interval is
a % of the price the user has determined. 
@author: Amanda
'''
import boto.ec2
import logging
import time
import datetime
import os
import calendar
from iso8601 import parse_date
from im_connector import * 
from config import * # aqui estan definidas las variables

HOUR_DURATION = 3600

# lista que almacena el ultimo precio historico de las spot
historical_price = [0,0]

# Metodo para convertir el timestamp de amazon (ISO 8601) en formato unix
def iso2unix(timestamp):
	# use iso8601.parse_date to convert the timestamp into a datetime object.
	parsed = parse_date(timestamp)
	# now grab a time tuple that we can feed mktime
	timetuple = parsed.timetuple()
	# return a unix timestamp
	return calendar.timegm(timetuple)

	
# Metodo principal
def is_checkpoint_time(launch_time, hostname):
	checkpoint = False
	
	#############################################################################
	# Por un lado comprobamos si ya ha pasado una hora para hacer el checkpoint #
	#############################################################################
	
	# Transformamos el timestamp en el formato manejado por python - NO hace falta
	launched = datetime.datetime.fromtimestamp(int(launch_time)).strftime('%Y-%m-%d %H:%M:%S')  
	logging.info("THRESHOLD: Time node " + hostname + " was launched is " + launched)
	# Obtenemos la hora actual
	now = time.strftime("%H:%M:%S") # En formato string          
	logging.info("THRESHOLD: Actual time is " + now) 
	actual_time = int(time.time())
	
	# Calculamos el tiempo que lleva el nodo en marcha
	live_time = actual_time - launch_time
	
	# Comprobamos si hay que hacer checkpoint
	remaining_hour_time = HOUR_DURATION - live_time % HOUR_DURATION
	logging.debug("THRESHOLD: Remaining hour time = %d for node %s" % (int(remaining_hour_time), hostname))
	if int(remaining_hour_time) < int(CKPT_TIME_MARGIN):
		checkpoint = True
		return checkpoint
	
	#####################################################################################################
	# Por otro, comprobamos si se ha producido una subida en el precio de las spot dentro del intervalo #
	#####################################################################################################
	
	# Para contactar con EC2 a traves de boto necesito la region, el auth_data, el tipo de instancia y la availability_zone
	# Obtengo la region
	region = get_region(hostname)
	logging.info("THRESHOLD: The region used to connect with AWS is " + region)
	# Obtengo el tipo de instancia 
	instance_type = get_instance_type(hostname)
	logging.info("THRESHOLD: The instance type to check the spot prices is " + instance_type) 
	
	# Obtengo la availability_zone
	availability_zone= get_availability_zone(hostname)
	logging.info("THRESHOLD: The availability zone to check the spot prices is " + availability_zone) 
	# Obtenemos el access key y el secret key del usuario para conectar con EC2
	auth_data = get_credentials()
	if auth_data:
		for auth in auth_data:
			if auth["type"]=='EC2':
				#logging.info(auth)
				access_key = auth["username"]
				secret_key= auth["password"]
	else:
		logging.error("THRESHOLD: Error obtaining user credentials for EC2")
	
	# El precio que el usuario puso (bid) lo puedo sacar con el IM: 
	bid = get_user_spot_bid(hostname)
	logging.info("THRESHOLD: The user's bid is " + str(bid))
	
	# Y el porcentaje para el intervalo esta en la variable THRESHOLD del config.py
	#Calcular el valor del threshold en base a un porcentaje que tenemos que estudiar para justificar en el paper 
	res = bid * THRESHOLD/100
	limit = bid - res
		
	# Definimos las variables necesarias para conectar con EC2
	os.environ['AWS_ACCESS_KEY_ID'] = access_key
	os.environ['AWS_SECRET_ACCESS_KEY'] = secret_key
	
	# Creamos la conexion a EC2
	ec2 = boto.ec2.connect_to_region(region)
	
	# get_spot_price_history(start_time=None, end_time=None, instance_type=None, product_description=None, availability_zone=None, dry_run=False, max_results=None, next_token=None, filters=None)
	# returns: A list tuples containing price and timestamp
	# Es un objeto tipo class 'boto.resultset.ResultSet' -> [SpotPriceHistory(m1.small):0.090000, SpotPriceHistory(m1.small):0.060000]
	# En el que el primer valor es el mas reciente y el otro es el anterior
	history = ec2.get_spot_price_history(instance_type=instance_type, availability_zone=availability_zone, max_results=1)
	if history:
		logging.debug("THRESHOLD: Current spot price for the availability zone " + availability_zone + " : " + str(history[0].price) + " at " + str(history[0].timestamp))
		# Consultamos nuestro precio almacenado
		# Si tenemos precio almacenado (solo no tendremos si es la primera vez que se ejecuta)
		if(historical_price[0] != 0 and historical_price[1] != 0):
			# Si ha cambiado el precio que teniamos almacenado (es decir, ha habido variacion en el precio de las spot)
			if historical_price[0] != str(iso2unix(history[0].timestamp)):
				# Tendremos que hacer checkpoint si el precio spot ha subido dentro del intervalo [limit, bid]
				if history[0].price > float(historical_price[1]):
					if history[0].price > limit:
						logging.info("THRESHOLD: A variation in the spot price inside the interval has been detected")
						checkpoint = True
			
		# Siempre sobreescribimos la lista con el ultimo valor que devuelve Amazon
		historical_price[0] = iso2unix(history[0].timestamp)
		historical_price[1] = history[0].price
	else:
		logging.error("THRESHOLD: Cannot get the current spot price from Amazon")
	
	return checkpoint

