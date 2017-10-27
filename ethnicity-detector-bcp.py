import pandas as pd
import json

from datetime import datetime, timedelta
import time
import arrow

from collections import defaultdict, Counter
from unidecode import unidecode

from string import ascii_lowercase

import sqlalchemy as sa

# for sending an email notification:
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import schedule

import pymssql

from multiprocessing
import numpy as np

from ethnicitydetector import EthnicityDetector

import subprocess
import os

def timer(func):
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
                        target_table='CustomerEthnicities', chsize=200000):
        
        self.SRC_TABLE = src_table   # where we take customer id and name from 
        self.TARGET_TABLE = target_table

        self._ENGINE = sa.create_engine('mssql+pymssql://{}:{}@{}:{}/{}'.format(user, user_pwd, server, port, db_name))
        
        self.TEMP_TABLE = "tmptable"

        self.TODAY_SYD = arrow.utcnow().to('Australia/Sydney').format('DD-MM-YYYY')
        self.LAST_SEVEN_DAYS = " ".join(['BETWEEN', "'" + (datetime.now() + timedelta(days=-6)).strftime("%Y%m%d") + "'", "AND", "'" +
        datetime.now().strftime("%Y%m%d") + "'"])
        
        self.QRY_TIMESPAN = {"last_7_days": 
                            {"descr": "within last seven days, " + self.LAST_SEVEN_DAYS.lower(),
                                "qry": """((([ModifiedDate] >= [CreatedDate]) AND ([ModifiedDate] {}))
                    OR ([CreatedDate] {}) ) AND ([CustomerListID] = 2)""".format(*[self.LAST_SEVEN_DAYS]*2)},
                    "before_today":
                    {"descr": "before today",
                    "qry": """((([ModifiedDate] >= [CreatedDate]) AND ([ModifiedDate]
                    <= {})) OR ([CreatedDate] <= {}) ) AND ([CustomerListID] = 2)""".format(*["'" + (datetime.now() +
                        timedelta(days=-1)).strftime("%Y%m%d") + "'"]*2)}}  

        self._detected_ethnicities = pd.DataFrame()

        # activate ethnicity detector instance
        ed = EthnicityDetector()

        # vectorized function from ed to actually detect ethnicities (when applied to an array)
        vf = np.vectorize(ed.get_ethnicity)

        # bcp
        self.BCP_OPTIONS = {"full_path": "", "format_file": "ethnicities.fmt", "temp_csv_file": "tmp_ethns.csv", "server": server}

 
    # wrapper around pandas' to_sql

    @timer
    def  _push_to_sql(self, df_upload, into_table, eng):

        print('uploading {} to sql server...'.format(df_upload))
       
        df_upload.to_sql(into_table, eng, if_exists='append', index=False, dtype={"CustomerID": sa.types.String(length=20),
                                                                            "Ethnicity": sa.types.String(length=50),
                                                                            "AssignedOn": sa.types.String(length=10)},
                                                                            schema="TEGA.dbo",
                                                                            chunksize=None if len(df_upload) <= 200000 else 200000)

    def get_array_ethnicity(self, b):  

        """
        IN: numpy array b that has two columns, oned contains customer id and another a full name
        OUT: numpy array with teo columns: customer id and ethnicity
        
        !NOTE: there will be 'None' where no ethnicityhas been detected 
        """
    
        ets = vf(b[:,-1])  # we assume that the second column contains full names

        stk = np.hstack((b[:,0].reshape(b.shape[0],1), ets.reshape(b.shape[0],1)))
    
        return stk

    @timer
    def get_ethnicities_parallel(self):

        """
        apply get_array_ethnicity to a number of dataframe chunks in parallel and then gather the results
        """

        print("identifying ethnicities in parallel...")

        AVAIL_CPUS = multiprocessing.cpu_count()

        pool = multiprocessing.Pool(AVAIL_CPUS)

        self._detected_ethnicities = pd.DataFrame(np.vstack(pool.map(self.get_array_ethnicity, np.array_split(self._CUST_TO_CHECK.values, AVAIL_CPUS))),
                   columns=["CustomerID", "Ethnicity"], dtype=str).query('Ethnicity != "None"')

        pool.close()
        pool.join()

        return self

    @timer
    def proc_new_customers(self):
        
        """
        get all new customers of interest from Lotus and p[ut them into a data frame]
        """

        # first just get the number of interesting customers on Lotus
        NROWS_SRC = self._ENGINE.execute(" ".join(["SELECT COUNT (*) FROM", self.SRC_TABLE , "WHERE",
                                                        self.QRY_TIMESPAN["last_7_days"]["qry"]])).fetchone()[0]

        print('there are {} rows to analyse in {}...'.format(NROWS_SRC, self.SRC_TABLE))


        self._CUST_TO_CHECK = pd.read_sql("SELECT [CustomerID], "
                             "ISNULL([FirstName],'') + ' ' + ISNULL([MiddleName],'') + ' ' + ISNULL([LastName],'') as [full_name] "
                            "FROM " + self.SRC_TABLE + " WHERE " + self.QRY_TIMESPAN["last_7_days"]["qry"], 
                            self._ENGINE)

        self.get_ethnicities_parallel()

        print("found {} rows with some ethnicities".format(len(self._detected_ethnicities)))

        return self
    
    @timer
    def update_ethnicity_table(self):
         
        print("updating ethnicity table..")

        if len(self._detected_ethnicities) < 1:

            print("[WARNING]: no new ethnicities to upload!")

        else:

            t_start = time.time()
            # add timestamp
            self._detected_ethnicities["AssignedOn"] = self.TODAY_SYD

            print("uploadig new ethnicities to table {}".format(self.TEMP_TABLE))

            if len(self._detected_ethnicities) > 10000:

            	# create a bcp format file; arguments used to launch process may be a list or a string

            	while not os.path.exists(self.BCP_OPTIONS["format_file"]):

            		subprocess.run(self.BCP_OPTIONS["full_path"] + ' ' + 'TEGA.dbo.' + self.TEMP_TABLE+ ' format nul -f ' 
            			+ self.BCP_OPTIONS["format_file"]  + '-n -T -S ' + self.BCP_OPTIONS["server"])

            	print("created bcp format file")

            	self._detected_ethnicities.to_csv(self.BCP_OPTIONS["temp_csv_file"])

            	print("saved new ethnicities to {}".format(self.BCP_OPTIONS["temp_csv_file"]))
            	

            	print("uploading...")
            	# now upload the csv file we have just created to SQL server usong bcp and the format file
            	subprocess.run(self.BCP_OPTIONS["full_path"] + ' ' + 'TEGA.dbo.' + self.TARGET_TABLE + " in " + 
            		self.BCP_OPTIONS["temp_csv_file"]+ ' -T -S ' + 
            				self.BCP_OPTIONS["server"] + ' -f ' + self.BCP_OPTIONS["format_file"])
      			print("done")    

      		else:  	

            	# upload all detected ethnicities into a temporary table
            	self._detected_ethnicities.to_sql(self.TEMP_TABLE, self._ENGINE, 
            	    if_exists='replace', index=False, 
            	        dtype={"CustomerID": sa.types.String(length=20)}, 
            	        schema="TEGA.dbo")

            ROWS_TMP = self._ENGINE.execute("SELECT COUNT (*) FROM {};".format(self.TEMP_TABLE)).fetchone()[0]
                
            print("made a temporary table with {} rows [{:.0f} min {:.0f} sec]...".format(ROWS_TMP, *divmod(time.time() - t_start, 60)))
            self._ENGINE.execute("DELETE FROM " + self.TARGET_TABLE + " WHERE CustomerID in (SELECT CustomerID FROM {});".format(self.TEMP_TABLE))
            print("deleted cids already in ethnicity table [{:.0f} min {:.0f} sec]...".format(*divmod(time.time() - t_start, 60)))
            self._ENGINE.execute("INSERT INTO " + self.TARGET_TABLE + " SELECT * FROM " + self.TEMP_TABLE)
            print("update complete [{:.0f} min {:.0f} sec]...".format(*divmod(time.time() - t_start, 60)))
    
    
    def send_email(self):
        
        sender_email, sender_pwd, smtp_server, smpt_port, recep_emails = [line.split("=")[-1].strip() 
                                    for line in open("config/email.cnf", "r").readlines() if line.strip()]
        
        msg = MIMEMultipart()   
        
        msg['From'] = sender_email
        msg['To'] = recep_emails
        msg['Subject'] = 'ethnicities: customers created or modified {}'.format(self.QRY_TIMESPAN["last_7_days"]["descr"])
        
        dsample = pd.DataFrame()

        for k, v in Counter(self._detected_ethnicities['Ethnicity']).items():
            this_ethnicity = self._detected_ethnicities[self._detected_ethnicities.Ethnicity == k]
            ns = 3 if len(this_ethnicity) > 2 else 1
            dsample = pd.concat([dsample, this_ethnicity.sample(n=ns)])

        st_summary  = "-- new ethnic customer ids captured:\n\n" + \
                "".join(["{}: {}\n".format(ks.upper(), vs) for ks, vs in sorted([(k,v) 
                    for k, v in Counter(self._detected_ethnicities['Ethnicity']).items()], key=lambda x: x[1], reverse=True)])
        
        msg.attach(MIMEText(st_summary+ "\n-- sample:\n\n" + dsample.loc[:,["CustomerID", "FullName", "Ethnicity"]].to_string(index=False, justify="left",
            formatters=[lambda _: "{:<12}".format(str(_).strip()), lambda _: "{:<30}".format(str(_).strip()), lambda _: "{:<20}".format(str(_).strip())]), 'plain'))
        server = smtplib.SMTP(smtp_server, smpt_port)
        server.starttls()
        print('sending email notification...', end='')
        server.login(sender_email, sender_pwd)
        server.sendmail(sender_email, [email.strip() for email in recep_emails.split(";")], msg.as_string())
        print('ok')
        server.quit()

if __name__ == '__main__':

    tc = TableHandler(**json.load(open("config/conn-01.ini", "r")))

    def job():
        
        tc.proc_new_customers()
        
        tc.update_ethnicity_table()
        
        tc.send_email()

    schedule.every().day.at('22:00').do(job)
    
    while True:

        schedule.run_pending()
