'''
Created on 01/07/2014

Set of methods to connect with the IM and obtain the needed information.

@author: Amanda
'''

# imports de librerias de python
import xmlrpclib # equivale a "import xmlrpc.client", pero han actualizado el nombre en las nuevas versiones de python
import logging
import sys
import time
sys.path.append("/usr/local/im")

# imports de ficheros propios
from config import * # aqui estan definidas las variables
from IM.auth import Authentication
from IM.radl import radl_parse
#from ckptman_daemon import logger

def connect():
	#logging.debug('im_connector: they call me')
	
	# comprobamos que el fichero de credenciales existe y esta en formato correcto
	if AUTH_FILE is None:
		logging.error("The authentication file is mandatory")
	#try:
	auth_data = Authentication.read_auth_data(AUTH_FILE)
	if auth_data is None:
		logging.error("The authentication file has incorrect format.")
	#except:
		#logging.error("The authentication file does not exist.")
		#time.sleep(30)
	#logging.info("Auth_data: " + str(auth_data))
	# conectamos con el demonio del IM   
	if XMLRCP_SSL:
		logging.debug("Client safely connecting with: " + IM_URL)
		from springpython.remoting.xmlrpc import SSLClient
		server = SSLClient(IM_URL, XMLRCP_SSL_CA_CERTS)
	else:
		logging.debug("Client connecting with: " + IM_URL)
		server = xmlrpclib.ServerProxy(IM_URL,allow_none=True)
		
	return auth_data, server
	
def get_credentials():
	# comprobamos que el fichero de credenciales existe y esta en formato correcto
	if AUTH_FILE is None:
		logging.error("The authentication file is mandatory")

	auth_data = Authentication.read_auth_data(AUTH_FILE)
	if auth_data is None:
		logging.error("The authentication file has incorrect format.")
		
	return auth_data

def getInfrastructureInfo():
	node_list = {}
	is_spot = False
	
	(auth_data, server) = connect()
	
	(success, res) = server.GetInfrastructureList(auth_data)
	if success:
		infrastructure_id = res[0]
		logging.debug("Obtained infrastructure " + str(infrastructure_id))
	else:
		logging.error("ERROR listing the infrastructures: " + res)
		sys.exit(1) 
	
	(success, res) = server.GetInfrastructureInfo(infrastructure_id, auth_data)
	vm_ids = res['vm_list']
	if success:
		logging.debug("Successfully obtained infrastructure info")
		for vm_id in vm_ids:
			(success, info)  = server.GetVMInfo(infrastructure_id, vm_id, auth_data)
		
			if success:
				info_radl = radl_parse.parse_radl(info)
				#instance_type = info_radl.systems[0].getValue('instance_type')
				is_spot = info_radl.systems[0].getValue('spot')
				node_name = info_radl.systems[0].name
				if is_spot == 'yes':
					node_list[node_name] = 'spot'
				else:
					node_list[node_name] = 'ondemand'
			else:
				logging.error("ERROR obtaining the node information: " + vm_id)
	
	logging.info("The node list that compose the infrastructure is: ") 
	logging.info(node_list)
	return node_list

# Metodo que el tiempo en el que un nodo se puso en marcha
def get_launch_time(node):
	launch_time = 0
		
	(auth_data, server) = connect()
	
	(success, res) = server.GetInfrastructureList(auth_data)
	if success:
		infrastructure_id = res[0]
		logging.debug("Obtained infrastructure " + str(infrastructure_id))
	else:
		logging.error("ERROR listing the infrastructures: " + res)
		sys.exit(1) 
	
	(success, res) = server.GetInfrastructureInfo(infrastructure_id, auth_data)
	vm_ids = res['vm_list']
	if success:
		logging.debug("Successfully obtained infrastructure info")
		for vm_id in vm_ids:
			(success, info)  = server.GetVMInfo(infrastructure_id, vm_id, auth_data)
		
			if success:
				info_radl = radl_parse.parse_radl(info)
				node_name = info_radl.systems[0].name
				if node_name == node:
					launch_time = info_radl.systems[0].getValue('launch_time')
			else:
				logging.error("ERROR obtaining the node information: " + vm_id)
	return launch_time

# Metodo que obtiene el estado de los nodos para saber si estan vivos o no
def get_node_state(node):
	state = ""

	(auth_data, server) = connect()
	
	(success, res) = server.GetInfrastructureList(auth_data)
	if success:
		infrastructure_id = res[0]
		logging.debug("Obtained infrastructure " + str(infrastructure_id))
	else:
		logging.error("ERROR listing the infrastructures: " + res)
		sys.exit(1) 
	
	(success, res) = server.GetInfrastructureInfo(infrastructure_id, auth_data)
	vm_ids = res['vm_list']
	if success:
		logging.debug("Successfully obtained infrastructure info")
		for vm_id in vm_ids:
			(success, info)  = server.GetVMInfo(infrastructure_id, vm_id, auth_data)
		
			if success:
				info_radl = radl_parse.parse_radl(info)
				node_name = info_radl.systems[0].name
				if node_name == node:
					state = info_radl.systems[0].getValue('state')
			else:
				logging.error("ERROR obtaining the node state: " + vm_id)
	
	return state

def get_user_spot_bid(node):
	bid = 0.0
		
	(auth_data, server) = connect()
	
	(success, res) = server.GetInfrastructureList(auth_data)
	if success:
		infrastructure_id = res[0]
		logging.debug("Obtained infrastructure " + str(infrastructure_id))
	else:
		logging.error("ERROR listing the infrastructures: " + res)
		sys.exit(1) 
	
	(success, res) = server.GetInfrastructureInfo(infrastructure_id, auth_data)
	vm_ids = res['vm_list']
	if success:
		logging.debug("Successfully obtained infrastructure info")
		for vm_id in vm_ids:
			(success, info)  = server.GetVMInfo(infrastructure_id, vm_id, auth_data)
		
			if success:
				info_radl = radl_parse.parse_radl(info)
				node_name = info_radl.systems[0].name
				if node_name == node:
					bid = info_radl.systems[0].getValue('price')
			else:
				logging.error("ERROR obtaining the user bid for the spot instance: " + vm_id)
	
	return bid

#Metodo que obtiene el tipo de instancia (small, micro..)
def get_instance_type(node):
	instance_type = ""

	(auth_data, server) = connect()
	
	(success, res) = server.GetInfrastructureList(auth_data)
	if success:
		infrastructure_id = res[0]
		logging.debug("Obtained infrastructure " + str(infrastructure_id))
	else:
		logging.error("ERROR listing the infrastructures: " + res)
		sys.exit(1) 
	
	(success, res) = server.GetInfrastructureInfo(infrastructure_id, auth_data)
	vm_ids = res['vm_list']
	if success:
		logging.debug("Successfully obtained infrastructure info")
		for vm_id in vm_ids:
			(success, info)  = server.GetVMInfo(infrastructure_id, vm_id, auth_data)
		
			if success:
				info_radl = radl_parse.parse_radl(info)
				node_name = info_radl.systems[0].name
				if node_name == node:
					instance_type = info_radl.systems[0].getValue('instance_type')
			else:
				logging.error("ERROR obtaining the instance type: " + vm_id)
	
	return instance_type

# La region la puedo sacar del disk.image.url (disk.0.image.url = 'aws://us-east-1/ami-e50e888c')
def get_region(node):
	region = ""

	(auth_data, server) = connect()
	
	(success, res) = server.GetInfrastructureList(auth_data)
	if success:
		infrastructure_id = res[0]
		logging.debug("Obtained infrastructure " + str(infrastructure_id))
	else:
		logging.error("ERROR listing the infrastructures: " + res)
		sys.exit(1) 
	
	(success, res) = server.GetInfrastructureInfo(infrastructure_id, auth_data)
	vm_ids = res['vm_list']
	if success:
		logging.debug("Successfully obtained infrastructure info")
		for vm_id in vm_ids:
			(success, info)  = server.GetVMInfo(infrastructure_id, vm_id, auth_data)
		
			if success:
				info_radl = radl_parse.parse_radl(info)
				node_name = info_radl.systems[0].name
				if node_name == node:
					image_url = info_radl.systems[0].getValue('disk.0.image.url')
					#region = parsear la url para quedarnos con ella ('aws://us-east-1/ami-e50e888c')
					region = image_url.split('/')[2]
			else:
				logging.error("ERROR obtaining the instance type: " + vm_id)
	
	return region
	
def get_availability_zone(node):
	availability_zone = ""

	(auth_data, server) = connect()
	
	(success, res) = server.GetInfrastructureList(auth_data)
	if success:
		infrastructure_id = res[0]
		logging.debug("Obtained infrastructure " + str(infrastructure_id))
	else:
		logging.error("ERROR listing the infrastructures: " + res)
		sys.exit(1) 
	
	(success, res) = server.GetInfrastructureInfo(infrastructure_id, auth_data)
	vm_ids = res['vm_list']
	if success:
		logging.debug("Successfully obtained infrastructure info")
		for vm_id in vm_ids:
			(success, info)  = server.GetVMInfo(infrastructure_id, vm_id, auth_data)
		
			if success:
				info_radl = radl_parse.parse_radl(info)
				node_name = info_radl.systems[0].name
				if node_name == node:
					availability_zone = info_radl.systems[0].getValue('availability_zone')
			else:
				logging.error("ERROR obtaining the instance type: " + vm_id)
	
	return availability_zone

