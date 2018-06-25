import re
import json
import time
import arrow
import os
import sys
import pandas as pd
import sqlalchemy
from sqlalchemy.orm.session import sessionmaker

def timer(func):
	"""
	decorator to time (table handling) functions/methods
	"""
	def wrapper(*args, **kwargs):

		t_start = time.time()
		res = func(*args, **kwargs)
		print("{} done in {:.0f} m {:.0f}s".format(func.__name__,  *divmod(time.time() - t_start, 60)))
		return res

	return wrapper

class TableHandler:
	
	"""
	class to connect to tables and get or upload stuff from/to tables
	"""
	def __init__(self, **kwargs):

		# we works in days so convert years to days if years are specified
		if 'days' in kwargs:
			self.DAYS = kwargs['days']
		elif 'years' in kwargs:
			self.DAYS = int(round(kwargs['years']*365,0))
		else:
			raise KeyError('TableHandler: you forgot to provide the days or years argument!')

		print(f'starting to collect new customers for the last {self.DAYS} days...')

		# describe the ethnicity table here
		self.ETHNICITY_COLS_AND_TYPES = [('CustomerID', 'INT NOT NULL'),
											('CleanCustomerName', 'VARCHAR(200)'), 
											('Ethnicity', 'VARCHAR(50)'), 
											('AssignedOn', 'VARCHAR(20)')]

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

		req_colnames = [t[0] for t in self.ETHNICITY_COLS_AND_TYPES[:-1]]

		if set(df.columns) != set(req_colnames):
			raise ValueError(f'[df2tab]: data frame must have the following columns: {", ".join(req_colnames)}!')

		timestamp_ = arrow.utcnow().to('Australia/Sydney').format("YYYY-MM-DD")

		# now create new temporary table

		self.sess.execute(f'CREATE TABLE {tab} ({",".join([" ".join(t) for t in self.ETHNICITY_COLS_AND_TYPES])})')

		print(f'created {tab}...')

		rows_ = len(df)

		_MAX_ROWS = 1000

		# there's a limit of **1,000** rows per query when attempting to insert as below
		# index i will vary from 0 to the number of 1,000-row chunks in df (note +1 because the right boundary is EXCLUDED in range)

		print(f'uploading data frame to {tab} by {_MAX_ROWS} rows...')

		for i in range(divmod(rows_, _MAX_ROWS)[0] + 1):

			vals_ = []

			for r in df.iloc[_MAX_ROWS*i:_MAX_ROWS*(i+1)].iterrows():

				# in sql server, single and double quotes are not the same unless forced, see
				# https://social.msdn.microsoft.com/Forums/sqlserver/en-US/b2443854-5f02-4ebe-ad8d-61725a95b2bf/
				# forum-faq-how-do-i-use-double-quotes-and-single-quotes-when-building-a-query-string?forum=sqldataaccess

				vals_.append('(' + str(r[1]['CustomerID']) + ',' + "'" + r[1]['CleanCustomerName'].replace("'","''") + "'" + ',' + "'" + r[1]['Ethnicity'] + "'" + ',' + "'" + timestamp_ + "'" + ')')

			self.sess.execute(f'INSERT INTO  {tab} ({", ".join([t[0] for t in self.ETHNICITY_COLS_AND_TYPES])}) VALUES {",".join(vals_)}')

			if (i + 1) % 20 == 0:
				print(f'uploaded rows: {_MAX_ROWS*(i+1)} ({100*_MAX_ROWS*(i+1)/rows_:.1f}%)')

		print('finished uploading')

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
		"""
		copy the contents of a temporary table tmp_tab into a table tab;
		then drop tmp_tab 
		"""
		
		if not self.exists(tmp_tab):
			raise Exception(f'temporary table {tmp_tab} doesn\'t exist!')

		if self.count_rows(tmp_tab) == 0:
			print(f'temporary table {tmp_tab} has no rows!') 
			return self

		if not self.exists(tab):

			self.sess.execute(f""" CREATE TABLE {tab} 
									({", ".join([" ".join(t) for t in self.ETHNICITY_COLS_AND_TYPES])})
								""")

			print(f'created table {tab}...')

		print(f'table {tab} has {self.count_rows(tab):,} rows...') 

		self.sess.execute(f"""DELETE FROM {tab} WHERE CustomerID IN 
										(SELECT CustomerID FROM {tmp_tab})""")

		self.sess.execute(f"""
							INSERT INTO {tab} ({", ".join([t[0] for t in self.ETHNICITY_COLS_AND_TYPES])})
							SELECT {", ".join([t[0] for t in self.ETHNICITY_COLS_AND_TYPES])} 
							FROM
							{tmp_tab};
							""")

		self.sess.execute(f'DROP TABLE {tmp_tab}')
		print(f'dropped temporary table {tmp_tab}')

		print(f'rows after update: {self.count_rows(tab):,}')

		return self