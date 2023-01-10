import discord
import asyncio

from os     import environ
from dotenv import load_dotenv

from aws_hook import MessageDB
import boto3

from datetime import datetime

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval  import IntervalTrigger

class DiscordMessenger(discord.Client):
	"""
	Encapsulates a Discord Client to send messages via DM and shared channels.
	"""

	def __init__(self, *args, **kwargs):
		"""
		:param: Discord API Client params
		"""
		super().__init__(*args, **kwargs)

		self.__setup_message_db(environ["TABLE_NAME"])
		self.__help_text = self.__load_file("help_text.txt")

		self.scheduler = AsyncIOScheduler()

	async def on_ready(self):
		"""
		Automatically runs when Client is successfully connected
		"""
		print(f'Logged in as {self.user} (ID: {self.user.id})')
		print('------')

		await self.__sync_scheduler()
		self.scheduler.start()
	
	async def on_message(self, message):
		"""
		Automatically called when a message of the correct format is read.
		Parses and executes the requested command.

		:param message: Message object read from discord.
		"""
		try:
			if not message.content or message.author == self.user:
				return

			cmd = message.content.split()
			if client.user.mentioned_in(message):
				cmd = cmd[1:]

			match cmd:
				case ["list"]:
					await self._execute_list(message)
				case ["add" | "send" | "spam", recipient, interval, *data]:
					self._execute_add(message, recipient, interval, data)
				case ["update", timestamp, interval] :
					self._execute_update(message, timestamp, interval)
				case ["delete" | "remove", *timestamp] :
					await self._execute_delete(message, " ".join(timestamp))
				case ["pause" | "deactivate", *timestamp]  :
					self._execute_deactivate(message.author, " ".join(timestamp))
				case ["unpause" | "activate", *timestamp]:
					self._execute_activate(message.author, " ".join(timestamp))
				case ["help"]   :
					await self._execute_help(message)
				case _ :
					pass

		except Exception as e:
			print(e)

	async def send_message(self, recipient, channel, message=""):
		"""
		Sends a given message to the recipient through a mention in the given channel.
		If no channel is given, recipient recieves the message through a dm.

		:param recipient: Recipient as a Discord.User object.
		:param message  : Message to be sent.
		:param image_url: Image to embed with message.
		:param channel  : Channel to mention recipient.
		:return: Response of message delivery. 
		"""

		#TODO handle embeds / image_url in here, as part of message
		await self.wait_until_ready()
		package = ""

		print(f"Sending message to {recipient.name}")

		endpoint = recipient
		if channel != None and not isinstance(channel, discord.DMChannel):
			endpoint = channel
			package = f"{recipient.mention} "
		
		# if image_url is not None:
		# 	embed = discord.Embed(color = 0x303136, description = message)
		# 	embed.set_image(url = image_url)
		# 	await endpoint.send(package, embed = embed)
		# else:
		package += message
		await endpoint.send(package)

	# ----- Command List -----
	async def _execute_list(self, message):
		"""
		Queries the database for all of author's saved information.
		Sends information on author's saved information to channel.

		:param message: Discord message object containing all relevant information.
		"""
		print(f"Listed {message.author.name} ({message.author.id})'s saved data in {message.channel}")

		response = self.message_db.lookup("user", str(message.author.id))

		out  = "```\n"
		out += "List of all current messages\n"
		out += "------------------------------\n"
		for row in response:
			out += f"{row['date_created']} {row['status']} {row['recipient_name']} ({row['interval']}s): {row['message']}\n" 		
		out += "```"

		await self.send_message(message.author, message.channel, out)

	def _execute_add(self, message, recipient, time_interval, data):
		"""
		Adds a message to the DynamoDB database and active scheduler.

		:param message      : Discord.Message object containing original message.
		:param recipient    : Recipient of message. Currently ignored, and using mention data.
		:param time_interval: Time interval in seconds (string).
		:param data         : Message to post, split by whitespace (list).
		"""
		if len(messsage.mentions) != 1:
			print("Invalid recipients for add")
			return

		recipient = message.mentions[0]

		out = " ".join(data)
		time_interval = int(time_interval)
		print(f"{message.author} is now sending {recipient.name} ({recipient.id}) : |{out}| every {time_interval} seconds")
		try:
			date_created = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
			# Add to DynamoDB Database
			self.message_db.add(
				date_created,
				message.author.name,
				out,
				recipient.name,
				recipient.id,
				time_interval,
				"Active"
			)

			# Add to Scheduler
			self.scheduler.add_job(
				self.send_message,
				args = [recipient, message.channel, out],
				trigger = "interval",
				seconds = time_interval,
				id = str(message.author.id) + date_created
			)

		except Exception as e:
			print(e)

	# TODO Error handle
	def _execute_update(self, message, timestamp, time_interval):
		"""
		TODO: Take time interval as a list and parse the time
		
		Updates an existing scheduled message.

		:param message      : Discord.Message object containing original message.
		:param timestamp    : Timestamp of message to be updated (string).
		:param time_interval: Time interval in seconds (string).

		"""
		print(f"{message.author} is updating {timestamp} to send every {time_interval} seconds")

		self.message_db.update(str(message.author.id), timestamp, "interval", time_interval)

		self.scheduler.reschedule_job(
			str(message.author.id) + timestamp,
			trigger = "interval",
			seconds = time_interval
		)

	# TODO Error handle remove_job
	async def _execute_delete(self, message, timestamp):
		"""
		Sets the message matching the author and date_time to be deleted. 
		
		:param message  : Discord.Message object containing original message.
		:param timestamp: DateTime string in the format of %Y-%m-%d %H:%M:%S of message to be deleted. 
		"""
		print(f"{message.author} is marking {timestamp} as deleted")
		if self.message_db.delete(str(message.author.id), timestamp):
			await self.send_message(message.author, message.channel, "Message deleted")
		else:
			await self.send_message(message.author, message.channel, "Timestamp does not exist")

		self.scheduler.remove_job(str(message.author.id) + timestamp)

	def _execute_deactivate(self, author, timestamp):
		"""
		Sets a scheduled message to be deactivated.
		
		:param author   : User requesting the deactivation.
		:param timestamp: Timestamp of message to be deactivated (string).
		"""
		print(f"{author} is deactivating {timestamp}")
		self.message_db.update(str(author.id), timestamp, "status", "Pause")
		self.scheduler.pause_job(job_id = str(author.id) + timestamp)
	
	# TODO Docstring
	def _execute_activate(self, author, timestamp):
		"""
		Sets a scheduled message to be activated.
		
		:param author   : User requesting the activation.
		:param timestamp: Timestamp of message to be deactivated (string).
		"""
		print(f"{author} is reactivating {timestamp}")
		self.message_db.update(str(author.id), timestamp, "status", "Active")
		self.scheduler.resume_job(job_id = str(author.id) + timestamp)
	
	async def _execute_help(self, message):
		"""
		Sends information regarding this bot to the specified location.

		:param message: Discord.Message object containing original message.
		"""
		print(f"{message.author} is requesting bot information in {message.channel}")
		await self.send_message(message.author, message.channel, self.__help_text)

	# Utilities
	def __load_file(self, file_name):
		"""
		Reads a file and returns its contents as a string.
		
		:param file_name: Name of file to read.
		:return: A single string containing the file data.
		"""
		with open(file_name, "r") as fd:
			return fd.read()
		return ""

	# ----- Database Methods -----
	def __setup_message_db(self, table_name):
		"""
		Sets up a connection to the DynamoDB database using environment settings.

		:param table_name: Name of table to connect to.
		"""
		self.message_db = MessageDB(
			dyn_resource=boto3.resource("dynamodb"),
			dyn_client=boto3.client("dynamodb"),
			region_name=environ["REGION_NAME"]
		)
		if not self.message_db.table_exists(table_name):
			self.message_db.create_table(table_name)
		self.message_db.load_table(table_name)
	
	# ----- Scheduler Methods -----
	async def __sync_scheduler(self):
		"""
		Sets up APScheduler on startup.
		"""
		for item in self.message_db.load_all():
			recipient = await client.fetch_user(item["recipient_id"])
			channel = None
			if recipient.id != item["channel_id"]:
				channel = self.get_channel(int(str(item["channel_id"])))

			self.scheduler.add_job(
				self.send_message,
				args = [recipient, channel, item["message"]],
				trigger = "interval",
				seconds = int(str(item["interval"])),
				id = item["user"] + item["date_created"]
			)
			if item["status"] == "Paused":
				self.scheduler.pause_job(job_id = item["user"] + item["date_created"])

if __name__ == "__main__":
	load_dotenv()
	client = DiscordMessenger(intents=discord.Intents.default())
	client.run(environ["DISCORD_TOKEN"])