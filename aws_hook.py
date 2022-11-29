
import boto3
import botocore

from datetime import datetime

# from scheduler import scheduler
#from schedule-trigger import Monday

# schedule = Scheduler()

class ClientError(Exception):
	"""Raises a client error for Messages"""
	pass

class Messages:
	"""Encapsulates an Amazon DynamoDB table of message data."""
	def __init__(self, dyn_resource=None, dyn_client=None, region_name=None):
		"""
		:param dyn_resource: A Boto3 DynamoDB resource.
		"""
		self.dyn_resource = dyn_resource
		self.dyn_client = dyn_client
		self.region_name = region_name
		self.table = None

	def create_table(self, table_name):
		"""
		Creates an Amazon DynamoDB table that can be used to store message data.
		The table uses the sender as the partition key and the
		date created as the sort key.

		:param table_name: The name of the table to create.
		:return: The newly created table.
		"""
		if self.dyn_resource is None:
			self.dyn_resource = boto3.resource('dynamodb')
		
		if self.dyn_client is None:
			self.dyn_client = boto3.client('dynamodb')

		try: 
			params = {
				'TableName': table_name,
				'KeySchema': [
					{'AttributeName': 'user', 'KeyType': 'HASH'},
					{'AttributeName': 'date_created', 'KeyType': 'RANGE'}
				],
				'AttributeDefinitions': [
					{'AttributeName': 'user', 'AttributeType': 'S'},
					{'AttributeName': 'date_created', 'AttributeType': 'S'}
				],
				'ProvisionedThroughput': {
					'ReadCapacityUnits': 10,
					'WriteCapacityUnits': 10
				}
			}
			self.table = self.dyn_resource.create_table(**params)
			print(f"Creating {table_name}...")
			self.table.wait_until_exists()
			print(f"Created table.")
		except self.dyn_client.exceptions.ResourceInUseException as err:
			print(f"Table already exists")
			self.table = self.dyn_resource.Table(table_name)
		except ClientError as err:
			print(f"Error: couldn't create table {table_name}")
			print(f"{err.response['Error']['Code']}: {err.response['Error']['Message']}")
			raise
		return self.table
	
	def load_table(self, table_name):
		"""
		Loads an AmazonDB table that can be used to store message data
		"""
		try:
			self.table = self.dyn_resource.Table(table_name)
			return self.table
		except ClientError as err:
			print(f"Error: {table_name} does not exist")
			print(f"{err.response['Error']['Code']}: {err.response['Error']['Message']}")
			raise

	
	def add(self, user, status, message, recipient, interval):
		"""
		Adds a message to the DynamoDB table.

		:param user     : The creator of the message.
		:param status   : Status of bot [active/paused/deleted].
		:param message  : Message to be sent.
		:param recipient: Recipient of he message.
		:param interval : Interval in minutes between sending messages.
		:return: Response of item creation.
		"""
		try:
			response = self.table.put_item(
				Item = {
					'user'         : user, # partition key
					'date_created' : datetime.now().strftime("%Y-%m-%d %H:%M:%S"), # sort key 
					'message'      : message,
					'recipient'    : recipient,
					'interval'     : interval,
					'status'       : status 
				}
			)
			print(f"Creating item...")
		except ClientError as err:
			raise
		return response
	
	# works but i should do mark delete instead
	def delete(self, user, date):
		response = self.table.delete_item(Key={
			"user"         : user,
			"date_created" : date
		})

	# Util
	def table_exists(self, name: str) -> bool:
		"""
		Checks for existence of [name] table.

		:param name: Name of table to check.
		:return: Boolean status.
		"""
		try:
			self.dyn_client.describe_table(TableName=name)
		except self.dyn_client.exceptions.ResourceNotFoundException:
			return False
		return True

if __name__ == '__main__':

	table_name = "Messages"
	messages = Messages(
		dyn_resource=boto3.resource("dynamodb"),
		dyn_client=boto3.client("dynamodb"),
		region_name="us-west-2"
	)
	if not messages.table_exists(table_name):
		messages.create_table(table_name)
	messages.load_table(table_name)
	
	# messages.add(
	# 	"Jo",
	# 	"active",
	# 	"Hi",
	# 	"Kain",
	# 	5
	# )
	# messages.delete("Jo", "2022-11-28 23:35:14")
	
	#print(messages.add_message