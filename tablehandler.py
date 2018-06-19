import re
import json
import time

# from ethnicity import Ethnicity

import arrow
import os
import sys
import pandas as pd
import sqlalchemy
from sqlalchemy.orm.session import sessionmaker

def timer(func):

	def wrapper(*args, **kwargs):

		t_start = time.time()
		res = func(*args, **kwargs)
		print("method: {} # elapsed time: {:.0f} m {:.0f}s".format(func.__name__, 
				*divmod(time.time() - t_start, 60)))
		return res

	return wrapper

class TableHandler(object):
	
	"""
	Class to connect to tables and get or upload stuff from/to tables
	"""
	def __init__(self, email=None, **kwargs):

		# we works in days so convert years to days if years are specified
		if 'days' in kwargs:
			self.DAYS = kwargs['days']
		elif 'years' in kwargs:
			self.DAYS = int(round(kwargs['years']*365,0))
		else:
			raise KeyError('you forgot to provide the days or years argument!')

	def start_session(self, sqlcredsfile):

		sql_creds = json.load(open(sqlcredsfile))

		sql_keys_required = set('user user_pwd server port db_name'.split())

		if sql_keys_required != set(sql_creds):
			raise KeyError(f'SQL Credentials are incomplete! The following keys are missing: '
				f'{", ".join([k for k in sql_keys_required - set(sql_creds)])}')

		# generate a Session object
		self._SESSION = sessionmaker(autocommit=True)
		self._ENGINE = sqlalchemy.create_engine(f'mssql+pymssql://{sql_creds["user"]}:{sql_creds["user_pwd"]}@{sql_creds["server"]}:{sql_creds["port"]}/{sql_creds["db_name"]}')
		self._SESSION.configure(bind=self._ENGINE)
		# create a session
		self.sess = self._SESSION()

		print('successfully started sqlalchemy session...')

		return self

	def close_session(self):

		self.sess.close()

		print('closed sqlalchemy session...')

		return self
 

	def __str__(self):

		"""
		returns what you see when attempting to print the class instance
		"""
		return('Table Handler')


	def exists(self, tab):
		"""
		check if a table tab exists; return 1 if it does or 0 otherwise
		"""
		return self.sess.execute(f""" IF OBJECT_ID(N'{tab}', N'U') IS NOT NULL
											SELECT 1
										ELSE
											SELECT 0
										  """).fetchone()[0]

	def count_rows(self, tab):
		"""
		count how many rows in table tab
		"""
		return self.sess.execute(f'SELECT COUNT (*) FROM {tab};').fetchone()[0]
				
	@timer
	def df2tab(self, df, tab):

		"""
		uploads table a pandas DataFrame df to a SQL table tab
		"""

		# does the target table even exist? if it does, drop it

		if self.exists(tab):

			self.sess.execute(f'DROP TABLE {tab}')

			print(f'dropped {tab}...')

		if set(df.columns) != set('CustomerID CleanCustomerName Ethnicity'.split()):
			raise ValueError(f'[df2tab]: data frame must have columns named CustomerID, CleanCustomerName and Ethnicity!')

		timestamp_ = arrow.utcnow().to('Australia/Sydney').format("YYYY-MM-DD")

		# now create new temporary table

		self.sess.execute(f'CREATE TABLE {tab} (CustomerID int NOT NULL, CleanCustomerName varchar(200), Ethnicity varchar(50), AssignedOn varchar(20))')

		print(f'created {tab}...')

		_MAX_ROWS = 1000

		# there's a limit of **1,000** rows per query when attempting to insert as below
		# index i will vary from 0 to the number of 1,000-row chunks in df (note +1 because the right boundary is EXCLUDED in range)

		print(f'uploading data frame to {tab} by {_MAX_ROWS} rows...')

		for i in range(divmod(len(df), _MAX_ROWS)[0] + 1):
			
			print(f'rows: {_MAX_ROWS*i} - {_MAX_ROWS*(i+1)}..')

			vals_ = []

			for r in df.iloc[_MAX_ROWS*i:_MAX_ROWS*(i+1)].iterrows():

				# in sql server, single and double quotes are not the same unless forced, see
				# https://social.msdn.microsoft.com/Forums/sqlserver/en-US/b2443854-5f02-4ebe-ad8d-61725a95b2bf/
				# forum-faq-how-do-i-use-double-quotes-and-single-quotes-when-building-a-query-string?forum=sqldataaccess

				vals_.append('(' + str(r[1]['CustomerID']) + ',' + "'" + r[1]['CleanCustomerName'].replace("'","''") + "'" + ',' + "'" + r[1]['Ethnicity'] + "'" + ',' + "'" + timestamp_ + "'" + ')')

			self.sess.execute(f' INSERT INTO  {tab} (CustomerID, CleanCustomerName, Ethnicity, AssignedOn) VALUES {",".join(vals_)}')

		print('done')

		return self


	@timer
	def get_new_cids(self, tab):
		
		"""
		get all new customers of interest from tab (Lotus) and put them into a data frame
		"""

		print(f'searching for new customers in {tab}...')

		# today as a time stamp
		_today_ts = arrow.utcnow().to('Australia/Sydney')

		print(f'date in Sydney: {_today_ts.format("YYYY-MM-DD")}')

		_days_ago_ts = _today_ts.shift(days=-self.DAYS)

		# self.BETWEEN_DAYS = f'BETWEEN \'{_days_ago_ts.format("YYYYMMDD")}\' AND \'{_today_ts.format("YYYYMMDD")}\''

		new_customerIDs = pd.read_sql(f"""
											SELECT CustomerID,
											ISNULL(FirstName,'') + ' ' + ISNULL(LastName,'') as FullName
											FROM {tab} 
											WHERE (CustomerID IS NOT NULL) AND 
											(
												((ModifiedDate >= CreatedDate) AND 
												(ModifiedDate BETWEEN \'{_days_ago_ts.format("YYYYMMDD")}\' AND \'{_today_ts.format("YYYYMMDD")}\'))
												OR
												(CreatedDate BETWEEN \'{_days_ago_ts.format("YYYYMMDD")}\' AND \'{_today_ts.format("YYYYMMDD")}\')

											)
											AND (CustomerListID = 2)
											""",
											self._ENGINE)

		print(f'new rows: {len(new_customerIDs)}')

		return new_customerIDs
	
	
	@timer
	def tmp2tab(self, tmp_tab, tab):
		
		if not self.exists(tmp_tab):
			raise Exception(f'temporary table {tmp_tab} doesn\'t exist!')

		if self.count_rows(tmp_tab) == 0:
			print(f'temporary table {tmp_tab} has no rows!') 
			return self

		if not self.exists(tab):

			self.sess.execute(f""" CREATE TABLE {tab} 
									(CustomerID int NOT NULL, 
									CleanCustomerName varchar(200), Ethnicity varchar(50), AssignedOn nvarchar(20))
								""")

			print(f'created table {tab}...')

		print(f'table {tab} has {self.count_rows(tab)} rows...') 

		self.sess.execute(f"""DELETE FROM {tab} WHERE CustomerID IN 
										(SELECT CustomerID FROM {tmp_tab})""")

		self.sess.execute(f"""
							INSERT INTO {tab} (CustomerID, CleanCustomerName, Ethnicity, AssignedOn)
							SELECT CustomerID, CleanCustomerName, Ethnicity, AssignedOn 
							FROM
							{tmp_tab};
							""")

		print(f'rows after update: {self.count_rows(tab)}')

		return self                
	
if __name__ == '__main__':

	tc = TableHandler(days=1).start_session(sqlcredsfile="config/conn-01.ini")

	newcids = tc.get_new_cids('DWSales.dbo.tbl_LotusCustomer')

	if len(newcids) > 0:

		e = Ethnicity().make_dicts()

		newcids = pd.concat([newcids[['CustomerID']], e.get(newcids['FullName'].tolist())[['Name', 'Ethnicity']]], 
								axis=1, ignore_index=True)
		newcids.columns = 'CustomerID CleanCustomerName Ethnicity'.split()

	print(newcids.head())

	tc.df2tab(newcids, 'TEGA.[tt\\igork].CustomerEthnicities_temp')

	tc.tmp2tab('TEGA.[tt\\igork].CustomerEthnicities_temp', 'TEGA.[tt\\igork].CustomerEthnicities_testing')

	tc.close_session()
