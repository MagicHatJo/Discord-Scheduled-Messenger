
import boto3
import botocore

from os     import environ
from dotenv import load_dotenv

from boto3.dynamodb.types     import TypeDeserializer
from boto3.dynamodb.transform import TransformationInjector

from datetime import datetime

class ClientError(Exception):
	"""
	Raises a client error for MessageDB
	"""
	pass

class MessageDB:
	"""
	Encapsulates an Amazon DynamoDB table of message data.
	"""
	def __init__(self, dyn_resource=None, dyn_client=None, region_name=None):
		"""
		:param dyn_resource: A Boto3 DynamoDB resource.
		:param dyn_client  : A Boto3 DynamoDB client.
		:param region_name : Region to connect to.
		"""
		self.dyn_resource = dyn_resource
		self.dyn_client   = dyn_client
		self.region_name  = region_name
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

		:param table_name: Name of table to be created.
		:return: Reference to successfully created table object.
		"""
		try:
			self.table = self.dyn_resource.Table(table_name)
			return self.table
		except ClientError as err:
			print(f"Error: {table_name} does not exist")
			print(f"{err.response['Error']['Code']}: {err.response['Error']['Message']}")
			raise
	
	def add(self, date_created, user, message, recipient_name, recipient_id, channel_id, interval, status="Active"):
		"""
		Adds a message to the DynamoDB table.

		:param user     : The creator of the message. (Partition key)
		:param status   : Status of bot [active/paused/deleted]. (Sort key)
		:param message  : Message to be sent.
		:param name     : Recipient of the message.
		:param id       : ID of recipient.
		:param interval : Interval in minutes between sending messages.
		:return: Response of item creation.
		"""
		try:
			response = self.table.put_item(
				Item = {
					'user'           : user,
					'date_created'   : date_created,
					'message'        : message,
					'recipient_name' : recipient_name,
					'recipient_id'   : recipient_id,
					'channel_id'     : channel_id,
					'interval'       : interval,
					'status'         : status 
				}
			)
		except ClientError as err:
			raise
		return response

	def delete(self, user, date):
		"""
		Marks a row in the DynamoDB table as deleted.

		:param user: Partition key.
		:param date: Sort key.
		:return: Boolean representing status of update.
		"""
		return self.update_status(user, date, "status", "Deleted")

	def update(self, user, date, key, new):
		"""
		Marks a row in the DynamoDB table as deleted.

		:param user  : Partition key.
		:param date  : Sort key.
		:param status: New status of item.
		:return: Boolean representing status of update.
		"""
		try:
			self.table.update_item(
				Key={
					"user"         : user,
					"date_created" : date
				},
				UpdateExpression="SET #st = :status_value",
				ConditionExpression='#usr = :user_value and #dt = :date_value and #st <> :status_value',
				ExpressionAttributeValues={
					":user_value"   : user,
					":date_value"   : date,
					":status_value" : new
				},
				ExpressionAttributeNames={
					"#usr" : "user",
					"#dt"  : "date_created",
					"#st"  : key
				},
				ReturnValues="UPDATED_NEW"
			)
			return True
		except Exception as e:
			return False

	def lookup(self, column, inquiry):
		"""
		Searches the DynamoDB table by column, and gets all rows matching the inquiry.

		:param column : The column in the table used for filtering. 
		:param inquiry: The value to filter by.
		:return: List of all non-deleted rows matching the inquiry.
		"""
		response = self.table.query(
			KeyConditionExpression=boto3.dynamodb.conditions.Key(column).eq(inquiry)
		)
		return list(filter(lambda x: x['status'] != "Deleted", response['Items']))
	
	def load_all(self):

		paginator     = self.dyn_client.get_paginator('scan')
		service_model = self.dyn_client._service_model.operation_model('Query')
		trans = TransformationInjector(deserializer = TypeDeserializer())

		scan_iterator = paginator.paginate(
			TableName=environ["TABLE_NAME"],
			PaginationConfig={
				"MaxItems": 5000,
				"PageSize": 10,
			}
		)

		for page in scan_iterator:
			trans.inject_attribute_value_output(page, service_model)
			for item in page["Items"]:
				if item["status"] != "Deleted":
					yield item


	# ----- Utilities -----
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

def debug_main():
	load_dotenv()

	# Create / Load Table
	message_db = MessageDB(
		dyn_resource=boto3.resource("dynamodb"),
		dyn_client=boto3.client("dynamodb"),
		region_name=environ["REGION_NAME"]
	)
	if not message_db.table_exists(environ["TABLE_NAME"]):
		message_db.create_table(environ["TABLE_NAME"])
	message_db.load_table(environ["TABLE_NAME"])

	message_db.add(
		"1971-1-1 00:00:00",
		"Sender",
		"Test Message",
		"Reciever",
		000000000000000000,
		999999999999999999,
		30,
		"Active"
	)
	# Test load_all
	for message in message_db.load_all():
		print(message)

	# Test add

	# Test lookup

	# Test update

	# Test delete

if __name__ == '__main__':
	debug_main()