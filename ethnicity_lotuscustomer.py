import pandas as pd
import json

from ethnicity import Ethnicity
from collections import defaultdict, Counter

# for sending an email notification:
import smtplib
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart

from jinja2 import Environment, FileSystemLoader

import os
import multiprocessing
import boto3

from tablehandler import TableHandler
from emailer import EthnicityEmailer


def split_df(df, chunks=2):

	"""
	this helper function splits a pandas data frame df into chunks; 
	returns a list of data frames
	"""

	df_rows = len(df)

	if df_rows < chunks:
		return [df]

	# df_rows >= chunks so pts_full >= 1

	rows_in_chunk, rows_left = divmod(df_rows, chunks)

	# if any rows left, i.e. rows_left > 0 then add an extra chunk

	return [df.iloc[i*rows_in_chunk:(i+1)*rows_in_chunk] for i in range(chunks + (rows_left > 0))]

def get_ethnicity_dataframe(x):
	"""
	data frame x must have columns called CustomerID and FullName; detect ethnicities for every name and
	return a data frame with columns CustomerID, CleanCustomerName, Ethnicity
	"""

	for col in 'CustomerID FullName'.split():
		if not col in x.columns:
			raise NameError(f'No {col} column found!')

	_ = pd.concat([x[['CustomerID']].reset_index(drop=True), 
			e.get(x['FullName'].tolist()).reset_index(drop=True)], axis=1) \
				.rename(columns={'Name': 'CleanCustomerName'})['CustomerID CleanCustomerName Ethnicity'.split()]

	_['CustomerID'] = _['CustomerID'].astype(int)
	_['CleanCustomerName'] = _['CleanCustomerName'].astype(str)
	_['Ethnicity'] = _['Ethnicity'].astype(str)

	return _

def get_ethnicity_parallel(x):
	"""
	divide data frame x between all available CPUs, detect ethnicities
	and gather the results back into a dingle data frame
	"""

	CPUS_ = multiprocessing.cpu_count()

	pool = multiprocessing.Pool(CPUS_)

	res = pd.concat(pool.map(get_ethnicity_dataframe, split_df(x, CPUS_)))

	pool.close()
	pool.join()

	return res

def create_jinja_mapping(x):

	d = defaultdict()
	
	for i, t in enumerate(Counter(x['Ethnicity']).most_common()[:3], 1):

		d[f'eth{i}'] = t[0]
		d[f'eth{i}_n'] = f'{t[1]:,}'

	return d

if __name__ == '__main__':

	LTUS_TAB = 'DWSales.dbo.tbl_LotusCustomer'
	TEMP_TAB = 'TEGA.dbo.CustomerEthnicities_temp'
	ETHN_TAB = 'TEGA.dbo.CustomerEthnicities'

	tc = TableHandler(years=20).start_session(sqlcredsfile="config/rds.txt")
	
	e = Ethnicity().make_dicts()

	MAX_NO_SUBSPLIT = 50000
	INTO_CHUNKS = 10

	newcids = tc.get_new_cids(LTUS_TAB)

	newrows_ = len(newcids)

	if newrows_ > 0:

		if newrows_ > MAX_NO_SUBSPLIT:

			n = newrows_ // INTO_CHUNKS  # chunk size

			print(f'new rows {newrows_:,} > {MAX_NO_SUBSPLIT:,}, split data frame into {INTO_CHUNKS:,} chunks ({n} rows each)...')
	
			num_chunks, extra = divmod(newrows_, n)
	
			dfs = []
	
			for j in range(0, num_chunks + (extra > 0)):

				from_ = j*n
				to_ = from_ + n
	
				print(f'chunk {j}: rows {from_} to {to_}...')

				res = get_ethnicity_parallel(newcids.iloc[from_:to_,:])
	
				dfs.append(res.query('Ethnicity != "---"'))

	
			allnew_ethnicities = pd.concat(dfs)

		else:  # we can simply divide between available CPUs

			allnew_ethnicities = get_ethnicity_parallel(newcids).query('Ethnicity != "---"')

		print(f'collected {len(allnew_ethnicities):,} new ethnic customer ids...')
  
		tc.df2tab(allnew_ethnicities, TEMP_TAB)
	
		tc.tmp2tab(TEMP_TAB, ETHN_TAB)
	
		tc.close_session()

		ee = EthnicityEmailer()

		ee.send_email_jinja(subj=f'[ethnicity update]: {len(allnew_ethnicities):,} new', 
								template_maps=create_jinja_mapping(allnew_ethnicities), 
								table_ref=f'see table {ETHN_TAB} for details', 
								creds_loc='local')

	else:

		print('no new ethnicities today, nothing to email about...')