import pandas as pd
import json
import time
import arrow

from tablehandler import TableHandler
from ethnicity import Ethnicity

from collections import defaultdict, Counter
from unidecode import unidecode

from string import ascii_lowercase

import sqlalchemy as sa
from sqlalchemy.orm.session import sessionmaker

# for sending an email notification:
import smtplib
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart

from jinja2 import Environment, FileSystemLoader

import pymssql

import sys
import os

import multiprocessing

import numpy as np

from ethnicitydetector import EthnicityDetector

import boto3


def split_df(df, chunks=2):

	"""
	split a pandas data frame df into chunks; 
	return a list
	"""

	df_rows = len(df)

	if df_rows < chunks:
		return [df]

	# df_rows >= chunks so pts_full >= 1

	rows_in_chunk, rows_left = divmod(df_rows, chunks)

	# if any rows left, i.e. rows_left > 0 then add an extra chunk

	return [df.iloc[i*rows_in_chunk:(i+1)*rows_in_chunk] for i in range(chunks + (rows_left > 0))]

def get_ethn(x):

	print(x[x['FullName'].astype(str) == 'nan'])

	_ = pd.concat([x[['CustomerID']], e.get(x['FullName'].tolist())[['Name', 'Ethnicity']]], axis=1, ignore_index=True)
	_.columns = 'CustomerID CleanCustomerName Ethnicity'.split()
	_ = _[_['CustomerID'].notnull()]

	_['CustomerID'] = _['CustomerID'].astype(int)
	_['CleanCustomerName'] = _['CleanCustomerName'].astype(str)
	_['Ethnicity'] = _['Ethnicity'].astype(str)

	return _

def send_email_sns(df, s3credsfile='config/creds-s3.json'):

        TOPIC_ARN = [l.strip() for l in open('config/arn.txt','r').readlines() if l.strip()].pop()

        boto3.client('sns', **json.load(open(s3credsfile, 'r'))).publish(TopicArn=TOPIC_ARN,
                                                Subject='new ethnicities', 
                                                    Message=f'Today\'s ethnicity update: {len(df)} new!')

def send_email_jinja(df):

    
    d = defaultdict()

    for i, t in enumerate(Counter(df['Ethnicity']).most_common()[:3], 1):
        d[f'eth{i}'] = t[0]
        d[f'eth{i}_n'] = f'{t[1]:,}'


    sender_email, sender_pwd, smtp_server, smpt_port, recep_emails = [line.split("=")[-1].strip() 
                                for line in open("config/email.cnf", "r").readlines() if line.strip()]

    email = MIMEMultipart('related')   
    
    email['From'] = sender_email
    email['To'] = recep_emails

    email['Subject'] = f"[ethnicity update] {len(df):,} new"
    

    file_loader = FileSystemLoader('templates')
    env = Environment(loader=file_loader)
    template = env.get_template('ethnicity_template.html')

    msg_text = template.render(**d, eth_tab='CustomerEthnicities')

    # main part is the Jinja2 template
    email.attach(MIMEText(msg_text,'html'))

    for i in range(1,4):

        p = d[f'eth{i}']

        flag_pic = f'img/{p}.png'

        if not os.path.exists(f'templates/{flag_pic}'):
            flag_pic = 'img/australian.png'

        flag_pic_full = f'templates/{flag_pic}'

        fp = open(flag_pic_full, 'rb')
        msg_img = MIMEImage(fp.read())
        fp.close()

        msg_img.add_header('Content-ID', f'<{flag_pic}>')
        msg_img.add_header('Content-Disposition', 'inline', filename=f'{flag_pic}')

        email.attach(msg_img)

    server = smtplib.SMTP(smtp_server, smpt_port)
    server.starttls()
    print('trying to login..')
    server.login(sender_email, sender_pwd)
    print('successfull')
    server.sendmail(sender_email, [email.strip() for email in recep_emails.split(";")], email.as_string())

    server.quit()


if __name__ == '__main__':

	tc = TableHandler(days=1).start_session(sqlcredsfile="config/conn-01.ini")
	
	e = Ethnicity().make_dicts()

	newcids = tc.get_new_cids('DWSales.dbo.tbl_LotusCustomer')

	if len(newcids) > 0:

		print(f'there are {len(newcids)} new customer ids...')

		AVAIL_CPUS = multiprocessing.cpu_count()

		print('available CPUs: ', AVAIL_CPUS)

		if len(newcids) > 1000:

			chunk_size = len(newcids) // 10

			print(f'since we have more than 1,000 cids, {len(newcids)}, we will go by {chunk_size}')
			
	
			num_chunks, extra = divmod(len(newcids), chunk_size)

			print(f'total {num_chunks} full {chunk_size}-row chunks.. and {extra} rows left')
	
			dfs = []
	
			for start in range(0, num_chunks + (extra > 0)):
				
				print('chunk ', start + 1)

				from_ = start*chunk_size
				to_ = from_ + chunk_size
	
				print(f'rows {from_} to {to_}...')
	
				pool = multiprocessing.Pool(AVAIL_CPUS)
	
				b = newcids.iloc[from_:to_,:]
	
				res = pd.concat(pool.map(get_ethn, split_df(b, AVAIL_CPUS)), ignore_index=True)

				print(res[res['CleanCustomerName'].astype(str) == 'nan'])
	
				pool.close()
				pool.join()
	
				dfs.append(res.query('Ethnicity != "---"'))

	
			allnew_ethnicities = pd.concat(dfs)
			print(f'collected {len(allnew_ethnicities)} new ethnic CIDs...')
			print(allnew_ethnicities.head())
	
  

	tc.df2tab(allnew_ethnicities, 'TEGA.[tt\\igork].CustomerEthnicities_temp')

	tc.tmp2tab('TEGA.[tt\\igork].CustomerEthnicities_temp', 'TEGA.[tt\\igork].CustomerEthnicities_testing')

	tc.close_session()

	# send_email_jinja(allnew_ethnicities)
	# send_email_sns(allnew_ethnicities)