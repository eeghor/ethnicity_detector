import pandas as pd
import json

import time
import arrow

from collections import Counter

import sqlalchemy as sa
from sqlalchemy import exc
from sqlalchemy.orm import sessionmaker

# for sending an email notification:
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import schedule

import pymssql

import numpy as np

from ethnicitydetector import EthnicityDetector

import subprocess
import sys
import os

from functools import wraps

def timer(func):

	@wraps(func)  # to preserve finction's metadata
	def wrapper(*args, **kwargs):
		t_start = time.time()
		res = func(*args, **kwargs)
		print("f: {} # elapsed time: {:.0f} m {:.0f}s".format(func.__name__.upper(), *divmod(time.time() - t_start, 60)))
		return res

	return wrapper

class TableHandler(object):
	
	"""
	Class to connect to tables and get or upload stuff from/to tables
	"""
	def __init__(self, server, user, port, user_pwd, db_name,
					src_table='[DWSales].[dbo].[tbl_LotusCustomer]', 
						tar_table='[TEGA].[dbo].[CustomerEthnicities]',
							days=4380):
		
		self.DAYS = days
		self.SRC_TABLE = src_table       # where we take customer id and name from 
		self.TAR_TABLE = tar_table

		self.CHNK = 40000  # process ethnicities by CHNK (in rows)
		self.NEWCUS_TBL = 'TEGA.dbo.tempNewCIDs'

		self.TAR_TABLE_COLUMN_NAMES = {"CustomerID", "FullName", "Ethnicity", "AssignedOn"}

		self.TAR_TABLE_FORMAT = "(CustomerID int, FullName nvarchar(100), Ethnicity nvarchar(50), AssignedOn nvarchar(20))"

		self._ENGINE = sa.create_engine('mssql+pymssql://{}:{}@{}:{}/{}'.format(user, user_pwd, server, port, db_name))
		
		# create a configured "Session" class
		Session = sessionmaker(bind=self._ENGINE, autocommit=True)
		self._SESSION = Session()
		
		# temp table to upload detected ethnicities to
		self.TEMP_TABLE = "TEGA.dbo.tmpEthn"

		self.TIMESPAN = "BETWEEN '{}' AND '{}'".format(arrow.utcnow().shift(days=-days).to('Australia/Sydney').format('YYYYMMDD'),
													arrow.utcnow().to('Australia/Sydney').format('YYYYMMDD'))


		self.WHERE_QRY = "WHERE ((([ModifiedDate] >= [CreatedDate]) AND ([ModifiedDate] {})) OR ([CreatedDate] {})) AND ([CustomerListID] = 2)".format(*[self.TIMESPAN]*2)
					

		# initialise a data frame with detected ethnicities now so that we can append to it
		self._detected_ethnicities = pd.DataFrame()

		# activate ethnicity detector instance
		self.ed = EthnicityDetector()

		# bcp options
		self.BCP_OPTIONS = {"full_path": "bcp", 
								"format_file": "ethnicities_fmt.xml", 
									"temp_new_cust_file": "tempNewCIDs.csv",
										"temp_csv_file": "tmp_ethns.csv", 
											"server": server}

	
	@timer
	def newcids_to_csv(self):
		"""
		find out which customer ids are of interest to us (and hence to be checked for ethnicity) and then collect these along with 
		the corresponding names in a temporary table
		"""

		print("checking {} for new customer ids to detect ethnicity for...".format(self.SRC_TABLE))
		nnc = self._SESSION.execute(" ".join(["SELECT COUNT (*) FROM", self.SRC_TABLE, self.WHERE_QRY])).fetchone()[0]
		print('eligible ids: {}'.format(nnc))

		self._recreate_table(self.NEWCUS_TBL, '(CustomerID int, FullName nvarchar(100))', 'create_new')

		self._SESSION.execute(("INSERT INTO " + self.NEWCUS_TBL + " SELECT CustomerID, "
			  "CAST (RTRIM(LTRIM(REPLACE( SUBSTRING( ISNULL([FirstName],'') + ' ' + ISNULL([MiddleName],'') + ' ' + ISNULL([LastName],''), 1, 100), '  ', ' '))) as nvarchar(50)) as [FullName] "
			  "FROM " + self.SRC_TABLE + " " + self.WHERE_QRY))

		nct = self._SESSION.execute(f"SELECT COUNT (*) FROM {self.NEWCUS_TBL}").fetchone()[0]

		assert nnc == nct, 'incorrect number of new customer ids in temporary table {}!'.format(self.NEWCUS_TBL)

		# now that this temp table is already there, download it using bcp
		subprocess.run(f"bcp {self.NEWCUS_TBL} out {self.BCP_OPTIONS['temp_new_cust_file']} -c -T -S {self.BCP_OPTIONS['server']}")

		print('downloaded ids and their full names to {}'.format(self.BCP_OPTIONS["temp_new_cust_file"]))

	
	@timer
	def get_ethnicities(self):
		"""
		simply apply ethnicity detector on a data frame column with full names
		"""

		print("detecting ethnicities...")

		for i, c in enumerate(pd.read_csv(self.BCP_OPTIONS["temp_new_cust_file"], sep='\t', dtype={"CustomerID": int, "FullName": str}, 
											names=["CustomerID", "FullName"], error_bad_lines=False, 
												chunksize=self.CHNK, encoding='latin-1', header=None)):

			c['Ethnicity'] = c["FullName"].apply(self.ed.get_ethnicity)
			# note that now c has columns CustomerID, FullName and Ethnicity
			c = c.loc[c.Ethnicity.notnull(),:]

			if len(c) > 0:
				self._detected_ethnicities = pd.concat([self._detected_ethnicities, c])

			print('ethnicity detector: processed {} customer ids...'.format((i+1)*self.CHNK))

		self._detected_ethnicities["AssignedOn"] = arrow.utcnow().to('Australia/Sydney').format('DD-MM-YYYY')
		self._detected_ethnicities.to_csv(self.BCP_OPTIONS["temp_csv_file"], index=False, sep='\t', encoding='latin-1')

		print("done. total {} ethnicities detected".format(len(self._detected_ethnicities)))

		return self

	def _recreate_table(self, table_name, table_fmt, if_exists='do_nothing'):

		try:
			self._SESSION.execute(" ".join(["CREATE TABLE", table_name, table_fmt]))
		except exc.OperationalError:  # this comes up if table elready exists
			# means drop didn't work
			if if_exists == 'create_new':
				self._SESSION.execute(" ".join(["DROP TABLE", table_name]))
				self._SESSION.execute(" ".join(["CREATE TABLE", table_name, table_fmt]))
			elif if_exists == 'do_nothing':
				pass

		print('re-created table {}'.format(table_name))
	
	@timer
	def update_ethnicity_table(self):

		print("updating ethnicity table {}...".format(self.TAR_TABLE))

		self._recreate_table(self.TEMP_TABLE, self.TAR_TABLE_FORMAT, 'create_new')

		nc_temp_table = self._SESSION.execute(f"SELECT COUNT (*) FROM {self.TEMP_TABLE}").fetchone()[0]

		# create a format file for bcp (from the temporary table, it's the same as format for target table)
		print('creating a format file for bcp...', end='')
		subprocess.run(f'bcp {self.TEMP_TABLE} format nul -x -f {self.BCP_OPTIONS["format_file"]}  -c -T -S {self.BCP_OPTIONS["server"]}')
		print("ok")

		subprocess.run(f'bcp {self.TEMP_TABLE} in {self.BCP_OPTIONS["temp_csv_file"]} -T -S {self.BCP_OPTIONS["server"]} -f {self.BCP_OPTIONS["format_file"]} -F 2')	
		
		nc_temp_table_now = self._SESSION.execute(f"SELECT COUNT (*) FROM {self.TEMP_TABLE}").fetchone()[0]
		
		# now append the ethnicities in temporary table to the target table (replace those already there)
		# does the target table even exist?

		sch = self.TAR_TABLE.split('.')[-2].replace('[','').replace(']','').strip()
		nm = self.TAR_TABLE.split('.')[-1].replace('[','').replace(']','').strip()

		# cname_tuples will be None if something goes wrong
		cname_tuples = self._SESSION.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE "
				f"(table_schema = '{sch}') and (table_name = '{nm}')").fetchall()
		if cname_tuples:
			target_colnames = {tp[0] for tp in cname_tuples}
			print('found the following column names in target table {}: {}'.format(target_colnames, self.TAR_TABLE))
			if target_colnames == self.TAR_TABLE_COLUMN_NAMES:   # all the right column names
				pass
			else:
				self._SESSION.execute(f"DROP TABLE {self.TAR_TABLE}")
				self._SESSION.execute(f"CREATE TABLE {self.TAR_TABLE} {self.TAR_TABLE_FORMAT}")	
		else:
			# if that didn't work out, assume it's because there's no target table
			try:
				self._SESSION.execute(f"CREATE TABLE {self.TAR_TABLE} {self.TAR_TABLE_FORMAT}")
				print("created new target table {}".format(self.TAR_TABLE))
			except exc.OperationalError: # looks like table does exist?
				print('target table {} appers to exist, something wrong.. '.format(self.TAR_TABLE))

		#self._recreate_table(self.TAR_TABLE, self.TAR_TABLE_FORMAT, 'do_nothing')

		nc_target_table_now = self._SESSION.execute(f"SELECT COUNT (*) FROM {self.TAR_TABLE}").fetchone()[0]
		print("rows in target table {} before update: {}".format(self.TAR_TABLE, nc_target_table_now))

		self._SESSION.execute(f"DELETE FROM {self.TAR_TABLE} WHERE CustomerID in (SELECT CustomerID FROM {self.TEMP_TABLE})")
		self._SESSION.execute(f"INSERT INTO {self.TAR_TABLE} SELECT * FROM {self.TEMP_TABLE}")
		self._SESSION.execute(f"DROP TABLE {self.TEMP_TABLE}")

		nc_target_table_after= self._SESSION.execute(f"SELECT COUNT (*) FROM {self.TAR_TABLE}").fetchone()[0]
		print("rows in target table {} after update: {}".format(self.TAR_TABLE, nc_target_table_after))

		print("new rows: {}".format(nc_target_table_after - nc_target_table_now))

		self._SESSION.close()

		os.remove(self.BCP_OPTIONS["temp_csv_file"])
		os.remove(self.BCP_OPTIONS["temp_new_cust_file"])

		print("done.")
	
	
	def send_email(self):

		print("sending email notification...", end='')
		
		sender_email, sender_pwd, smtp_server, smpt_port, recep_emails = [line.split("=")[-1].strip() 
									for line in open("config/email.cnf", "r").readlines() if line.strip()]
		
		msg = MIMEMultipart()   
		
		msg['From'] = sender_email
		msg['To'] = recep_emails
		msg['Subject'] = 'ethnicity update: {} new between {} and today'.format(len(self._detected_ethnicities), arrow.utcnow().shift(days=-self.DAYS).to('Australia/Sydney').humanize())
		
		if len(self._detected_ethnicities) < 1:

			msg.attach(MIMEText('no new ethnicities, nothing much to say..', 'plain'))

		else:

			dsample = pd.DataFrame()
	
			for k, v in Counter(self._detected_ethnicities['Ethnicity']).items():
				
				# recall "CustomerID", "FullName", "Ethnicity", "AssignedOn"
				this_ethnicity = self._detected_ethnicities[self._detected_ethnicities.Ethnicity == k]
				ns = 3 if len(this_ethnicity) > 2 else 1
				dsample = pd.concat([dsample, this_ethnicity.sample(n=ns)])
	
			
			dsample["FullName"] = dsample["FullName"].str.upper()

			st_summary  = "-- new ethnic customer ids captured:\n\n" + \
					"".join(["{}: {}\n".format(ks.upper(), vs) for ks, vs in sorted([(k,v) 
						for k, v in Counter(self._detected_ethnicities['Ethnicity']).items()], key=lambda x: x[1], reverse=True)])
			
			msg.attach(MIMEText(st_summary+ "\n-- sample:\n\n" + dsample.loc[:,["CustomerID", "FullName", "Ethnicity"]].to_string(index=False, justify="left",
				formatters=[lambda _: "{:<12}".format(str(_).strip()), lambda _: "{:<30}".format(str(_).strip()), lambda _: "{:<20}".format(str(_).strip())]), 'plain'))
		
		server = smtplib.SMTP(smtp_server, smpt_port)
		server.starttls()
		server.login(sender_email, sender_pwd)
		server.sendmail(sender_email, [email.strip() for email in recep_emails.split(";")], msg.as_string())
		print('ok')
		server.quit()


if __name__ == '__main__':

	tc = TableHandler(**json.load(open("config/conn-02.ini", "r")))

	def job():
		
		tc.newcids_to_csv()

		tc.get_ethnicities()
		
		tc.update_ethnicity_table()
		
		tc.send_email()

	schedule.every().day.at('12:48').do(job)
	
	while True:

		schedule.run_pending()
