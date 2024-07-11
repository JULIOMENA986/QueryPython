
from pandas import pandas as pd
from mysql import connector as mysql
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import create_engine



class Connection:
	def __init__(self, database):
		self.cursor = None
		self.connection = None
		self.database = database
		self.host = '127.0.0.1'
		self.user = 'Mena'
		self.password = 'CESARmena1806'
		self.port = '3306'

	def connect(self):
		# todo: db connection
		if self.connection is None:
			try:
				self.connection = create_engine(
					f'mysql+mysqldb://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}'
				)
			except Exception as err:
				print(f"Error: {err}")
				self.connection = None
		# if self.connection is None:
		# 	try:
		# 		self.connection = mysql.connect(
		# 			host=self.host,
		# 			user=self.user,
		# 			password=self.password,
		# 			port=self.port,
		# 			database=self.database
		# 		)
		# 		print(f"Connected to {self.database}")
		#
		# 	except mysql.Error as err:
		# 		print(f"Error: {err}")
		# 		self.connection = None

	def execute_query(self, query):
		self.connect()
		# if self.connection:
		# 	cursor = self.connection.cursor()
		# 	try:
		# 		cursor.execute(query)
		# 		result = cursor.fetchall()
		# 		return result
		# 	except mysql.Error as err:
		# 		print(f"Error: {err}")
		# 		return None
		# 	finally:
		# 		cursor.close()
		if self.connection:
			with self.connection.connect() as connection:
				try:
					result = connection.execute(query)
					return result.fetchall()
				except Exception as err:
					print(f"Error: {err}")
					return None

	def fetch_dataframe(self, query):
		self.connect()
		# if self.connection:
		# 	try:
		# 		df = pd.read_sql(query, self.connection)
		# 		return df
		# 	except Exception as err:
		# 		print(f"Error: {err}")
		# 		return None
		if self.connection:
			try:
				df = pd.read_sql(query, self.connection)
				return df
			except Exception as err:
				print(f"Error: {err}")
				return None

	def close(self):
		# if self.connection and self.connection.is_connected():
		# 	self.connection.close()
			# self.connection = None
		if self.connection:
			self.connection.dispose()
			self.connection = None





