import discord
import asyncio

from os import environ
from dotenv import load_dotenv

# TODO: Cache user and channel
class ReminderBot(discord.Client):
	"""Encapsulates a Discord Client to send messages via DM and shared channels"""

	def __init__(self, *args, **kwargs):
		"""
		:param: Discord API Client params
		"""
		super().__init__(*args, **kwargs)

	async def on_ready(self):
		"""
		Automatically runs when Client is successfully connected
		"""
		print(f'Logged in as {self.user} (ID: {self.user.id})')
		print('------')

	async def send_message(self, recipient, message="", channel=None):
		"""
		Sends a given message to the recipient through a mention in the given channel.
		If no channel is given, recipient recieves the message through a dm.

		:param recipient: ID of recipient.
		:param message  : Message to be sent.
		:param channel  : Channel to mention recipient.
		:return: Response of message delivery. 
		"""
		await self.wait_until_ready()
		user = await client.fetch_user(recipient)
		if channel is not None:
			print(f"Found Channel {channel}")
			channel = self.get_channel(channel)
			print(f"Sending |{message}| to {user.name} in {channel}")
			await channel.send(f"{user.mention} {message}")
		else:
			print(f"Sending |{message}| to {user.name}")
			await user.send(message)
	
	async def send_message_on_repeat(self, recipient, message, interval, channel):
		"""
		Given an interval in seconds, automatically loops message delivery.
		"""
		await self.wait_until_ready()
		while not self.is_closed():
			await self.send_message(recipient, message)
			await asyncio.sleep(interval)  # interval in seconds

if __name__ == "__main__":
	load_dotenv()
	client = ReminderBot(intents=discord.Intents.default())
	client.run(environ["DISCORD_TOKEN"])