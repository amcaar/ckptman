import logging
import os
import boto.ec2

from im_connector import connect, get_credentials
from IM.radl import radl_parse 

class history_price:
	def __init__(self, time, price):
		self.timestamp = int(time)
		self.price = float(price)
	
	def __str__(self):
		return "%d, %f" % (self.timestamp, self.price)

class spot_mock:
	
	DATA_FILE = "/usr/local/ckptman/price.txt"

	def __init__(self):
		self.history = []
		try:
			f = open(self.DATA_FILE,'r')
			for line in f.readlines():
				parts = line.split("\t")
				self.history.append(history_price(parts[1], parts[0]))
			f.close()
		except:
			logging.exception("Error reading spot price history file: %s." + self.DATA_FILE)
	
	def get_spot_price_history(self, start, end):
		"""
		Get the spot price history in the specified time range
		"""
		max = history_price(-1,-1)
		res = []
		for v in self.history:
			if max.timestamp < v.timestamp and v.timestamp < end:
				max = v
			if v.timestamp >= start and v.timestamp <= end:
				res.append(v)
		
		res = sorted(res, reverse=True)
		if not res:
			res = [max]
		return res
	
	def vm_killer(self, timestamp):
		"""
		Kill all the VMs if the spot price in "timestamp" is higher than bid 
		"""
		vm_list = self._get_vm_list()
		if vm_list:
			# All the VMs have the same bid price, take the first one
			bid = float(vm_list[0].systems[0].getValue('price'))

			if bid <= 0:
				logging.warn("User bid is 0!. Skipping VM kill step.")
				return
			
			last_price = self.get_spot_price_history(0, timestamp)[0].price
	
			if last_price > bid:
				logging.info("Last price %f is higher than bid %f. Kill VMs!" % (last_price, bid))
				# group them by region
				vm_groups = {}
				for vm in vm_list:
					image_url = vm.systems[0].getValue('disk.0.image.url')
					# Parse the URL to get the region (for example 'aws://us-east-1/ami-e50e888c')
					region = image_url.split('/')[2]
					if region in vm_groups:
						vm_groups[region].append(vm)
					else:
						vm_groups[region] = [vm]
				
				self._set_ec2_credentials()
				
				# Kill them all!
				for region in vm_groups.keys():
					instances = []
					for vm in vm_groups[region]:
						instance_id = vm.systems[0].getValue('instance_id').split(";")[1]
						if instance_id.startswith("sir"):
							logging.warn("Trying to kill a spot request: %s. Ignore it." % instance_id)
						else:
							instances.append(instance_id)
					logging.info("Terminating instances: ")
					logging.info(instances)
					
					try:
						conn = boto.ec2.connect_to_region(region)
						conn.terminate_instances(instance_ids=instances)
					except:
						logging.exception("Error terminating EC2 instances")
			else:
				logging.debug("Last price %f is lower than bid %f." % (last_price, bid))
		else:
			logging.debug("No VMs to check.")

	@staticmethod
	def _set_ec2_credentials():
		auth_data = get_credentials()
		if auth_data:
			for auth in auth_data:
				if auth["type"]=='EC2':
					access_key = auth["username"]
					secret_key= auth["password"]
		else:
			logging.error("SPOT_MOCK: Error obtaining user credentials for EC2")
			
		os.environ['AWS_ACCESS_KEY_ID'] = access_key
		os.environ['AWS_SECRET_ACCESS_KEY'] = secret_key

	# Get the infrastructure info
	@staticmethod
	def _get_vm_list():
		"""
		Get the list of VMs from the IM
		"""
		vm_list = []
		
		(auth_data, server) = connect()
		
		(success, res) = server.GetInfrastructureList(auth_data)
		if success:
			infrastructure_id = res[0]
			logging.debug("Obtained infrastructure " + str(infrastructure_id))
		else:
			logging.error("ERROR listing the infrastructures: " + res) 
		
		(success, vm_ids) = server.GetInfrastructureInfo(infrastructure_id, auth_data)
		if success:
			logging.debug("Successfully obtained infrastructure info")
			for vm_id in vm_ids:
				(success, info)  = server.GetVMInfo(infrastructure_id, vm_id, auth_data)
			
				if success:
					info_radl = radl_parse.parse_radl(info)
					vm_list.append(info_radl)
				else:
					logging.error("ERROR obtaining the node information: " + vm_id)
		
		logging.debug("The vm list to check if they are going to be killed: ") 
		logging.debug(vm_list[1:])
		return vm_list[1:]
