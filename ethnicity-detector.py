import pandas as pd
import json
import time
import arrow

from collections import defaultdict, Counter
from unidecode import unidecode

from string import ascii_lowercase

import sqlalchemy as sa
from sqlalchemy.orm.session import sessionmaker

# for sending an email notification:
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import pymssql

import sys

import multiprocessing

import numpy as np

from ethnicitydetector import EthnicityDetector

def timer(func):
    def wrapper(*args, **kwargs):
        t_start = time.time()
        res = func(*args, **kwargs)
        print("f: {} # elapsed time: {:.0f} m {:.0f}s".format(func.__name__.upper(), *divmod(time.time() - t_start, 60)))
        return res
    return wrapper

def get_array_ethnicity(b):  

        """
        IN: numpy array b that has two columns, one contains customer id and another a full name
        OUT: numpy array with two columns: customer id and ethnicity
        
        !NOTE: there will be 'None' where no ethnicity has been detected 
        """
    
        ets = vf(b[:,-1])  # we assume that the second column contains full names

        stk = np.hstack((b[:,0].reshape(b.shape[0],1), ets.reshape(b.shape[0],1)))
    
        return stk

class TableHandler(object):
    
    """
    Class to connect to tables and get or upload stuff from/to tables
    """
    def __init__(self, server, user, port, user_pwd, db_name,
                    src_table='DWSales.dbo.tbl_LotusCustomer', 
                        target_table='TEGA.dbo.CustomerEthnicities_16', days=17, chsize=200000):
        
        self.SRC_TABLE = src_table   # where we take customer id and name from 
        self.TARGET_TABLE = target_table
        self.DAYS = days

        self.CUSTID = 'CustomerID'

        self._SESSION = sessionmaker(autoflush=False, autocommit=True)
        self._ENGINE = sa.create_engine(f'mssql+pymssql://{user}:{user_pwd}@{server}:{port}/{db_name}')
        self._SESSION.configure(bind=self._ENGINE)

        self.sess = self._SESSION()

        self.TEMP_TABLE = "TEGA.dbo.tempo_LARGE_table"

        # today as a time stamp
        _today_ts = arrow.utcnow().to('Australia/Sydney')
        # today as a string
        self.TODAY_SYD = _today_ts.format('DD-MM-YYYY')

        _days_ago_ts = _today_ts.shift(days=-self.DAYS)

        self.BETWEEN_DAYS = f'BETWEEN \'{_days_ago_ts.format("YYYYMMDD")}\' AND \'{_today_ts.format("YYYYMMDD")}\''
        
        self.QRY_TIMESPAN = f"""
                                (
                                  ((ModifiedDate >= CreatedDate) AND (ModifiedDate {self.BETWEEN_DAYS}))
                                    or
                                  (CreatedDate {self.BETWEEN_DAYS})

                                )
                                
                                and (CustomerListID = 2)
                            """

        self._detected_ethnicities = pd.DataFrame()

        # activate ethnicity detector instance
        # ed = EthnicityDetector()

        # vectorized function from ed to actually detect ethnicities (when applied to an array)
        # vf = np.vectorize(ed.get_ethnicity)
 
    # wrapper around pandas' to_sql

    @timer
    def  _push_to_sql(self, df_upload, into_table, eng):

        print('uploading {} to sql server...'.format(df_upload))
       
        df_upload.to_sql(into_table, eng, if_exists='append', index=False, 
            dtype={"CustomerID": sa.types.String(length=20),
                    "Ethnicity": sa.types.String(length=50),
                    "AssignedOn": sa.types.String(length=10)},
                    schema="TEGA.dbo",
                    chunksize=None if len(df_upload) <= 200000 else 200000)

    def exists(self, tab):
        """
        check if a table tab exists
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
        return self._ENGINE.execute(f'SELECT COUNT (*) FROM {tab};').fetchone()[0]


    @timer
    def get_ethnicities_parallel(self):

        """
        apply get_array_ethnicity to a number of data frame chunks in parallel and then gather the results
        """

        print("identifying ethnicities in parallel...")

        AVAIL_CPUS = multiprocessing.cpu_count()
        print(f'available CPUs: {AVAIL_CPUS}')

        print('creating a pool...', end='')
        pool = multiprocessing.Pool(AVAIL_CPUS)
        print('ok')

        self._detected_ethnicities = pd.DataFrame(np.vstack(pool.map(self.get_array_ethnicity, 
            np.array_split(self._CUST_TO_CHECK.values, AVAIL_CPUS))),
                    columns=["CustomerID", "Ethnicity"], dtype=str).query('Ethnicity != "None"')

        pool.close()
        pool.join()

        return self

    @timer
    def proc_new_customers(self):
        
        """
        get all new customers of interest from Lotus and put them into a data frame
        """

        # first just get the number of interesting customers on Lotus
        NROWS_SRC = self.sess.execute(f"""
                                            SELECT COUNT (*) FROM {self.SRC_TABLE} 
                                            WHERE {self.QRY_TIMESPAN}
                                          """).fetchone()[0]

        print('there are {} rows to analyze in {}...'.format(NROWS_SRC, self.SRC_TABLE))


        self._CUST_TO_CHECK = pd.read_sql(f"""
                                            SELECT {self.CUSTID},
                                            ISNULL(FirstName,'') + ' ' + ISNULL(MiddleName,'') + ' ' + ISNULL(LastName,'') as full_name
                                            FROM {self.SRC_TABLE} 
                                            WHERE {self.QRY_TIMESPAN}
                                            """,
                                            self._ENGINE)
        print(self._CUST_TO_CHECK.head())

        # self.get_ethnicities_parallel()

        # print("found {} rows with some ethnicities".format(len(self._detected_ethnicities)))

        return self
    
    @timer
    def update_ethnicity_table(self):
         
        print("updating ethnicity table..")

        if len(self._detected_ethnicities) < 1:

            print("[WARNING]: no new ethnicities to upload!")

        else:

            t_start = time.time()
            # add time stamp
            self._detected_ethnicities["AssignedOn"] = self.TODAY_SYD

            # upload all detected ethnicities into a temporary table
            print(f'attempting to run .to_sql to make table {self.TEMP_TABLE}')

            self._detected_ethnicities.to_sql(self.TEMP_TABLE, self._ENGINE, 
                if_exists='replace', index=False, 
                    dtype={"CustomerID": sa.types.String(length=20)}, 
                    schema="TEGA.dbo", 
                    chunksize=(None if len(self._detected_ethnicities) <= 1000 else 1000))
            print('ok')

            print(f'temp table has {self.count_rows(self.TEMP_TABLE)} rows')

            if self.exists(self.TARGET_TABLE):

                _b = self.count_rows(self.TARGET_TABLE)

                print(f'target table EXISTS and has {_b} rows')
            else:
                print('target table doesnt exist...')

                self.sess.execute(f"CREATE TABLE {self.TARGET_TABLE} (CustomerID int, Ethnicity nvarchar(50), AssignedOn nvarchar(20))")
                
                self.sess.execute(f'INSERT INTO {self.TARGET_TABLE} (CustomerID, Ethnicity) SELECT CustomerID, Ethnicity FROM {self.TEMP_TABLE}')
                # try:

                #     _b = self.count_rows(self.TARGET_TABLE)

                #     self.sess.execute(f"""
                #                     DELETE FROM {self.TARGET_TABLE} 
                #                     WHERE {self.CUSTID} in (SELECT {self.CUSTID} FROM {self.TEMP_TABLE});""")

                #     _a = self.count_rows(self.TARGET_TABLE)

                #     print(f'deleted {_a - _b} customer ids from {self.TARGET_TABLE}')

                # except:

                #     print('couldn\'t delete from the target table although it does exits!')
            if self.exists(self.TARGET_TABLE):

                _b = self.count_rows(self.TARGET_TABLE)

                print(f'NOW target table EXISTS and has {_b} rows')

            sys.exit(0)

            try:
                print("merging..")

                self.sess.execute(f"""
                                MERGE {self.TARGET_TABLE} AS TARGET
                                USING {self.TEMP_TABLE} AS SOURCE
                                ON TARGET.CustomerID = SOURCE.CustomerID
                                WHEN MATCHED AND (TARGET.Ethnicity <> SOURCE.Ethnicity)
                                THEN
                                UPDATE
                                SET TARGET.Ethnicity = SOURCE.Ethnicity
                                WHEN NOT MATCHED BY TARGET
                                THEN
                                INSERT (CustomerID, Ethnicity, AssignedOn)
                                VALUES (SOURCE.CustomerID, SOURCE.Ethnicity, SOURCE.AssignedOn);
                                """)
                print('done')
                _a = self.count_rows(self.TARGET_TABLE)
                print(f'target table NOW  has {_a} rows: {_a - _b} change')

            except:
                print('cant MERGE!')
            # else:

            #     print('does target exist?')
            #     print(self.exists(self.TARGET_TABLE))

                # self.sess.execute(f"""
                #                     SELECT * INTO {self.TARGET_TABLE}
                #                     FROM {self.TEMP_TABLE};
                #                     """)

                # print('done select into..')

                # print(self.exists(self.TARGET_TABLE))

                # print(f'SELECT COUNT (*) FROM {self.TARGET_TABLE};')

                # _a= self._ENGINE.execute(f'SELECT COUNT (*) FROM {self.TARGET_TABLE};').fetchone()[0]

            print(f'now rows in {self.TARGET_TABLE}: {self.count_rows(self.TARGET_TABLE)}')
                  
            
            print("update complete [{:.0f} min {:.0f} sec]...".format(*divmod(time.time() - t_start, 60)))
    
    
    def send_email(self):

        print("sending email notification...", end='')
        
        sender_email, sender_pwd, smtp_server, smpt_port, recep_emails = [line.split("=")[-1].strip() 
                                    for line in open("config/email.cnf", "r").readlines() if line.strip()]
        
        msg = MIMEMultipart()   
        
        msg['From'] = sender_email
        msg['To'] = recep_emails
        msg['Subject'] = 'ethnicity update: {} new between {} and today'.format(len(self._detected_ethnicities), arrow.utcnow().shift(days=-self.DAYS).to('Australia/Sydney').humanize())
        
        if len(self._detected_ethnicities) < 1:

            msg.attach(MIMEText('no new ethnicities, nothing to see here..', 'plain'))

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
                formatters=[lambda _: "{:<12}".format(str(_).strip()), 
                lambda _: "{:<30}".format(str(_).strip()), 
                lambda _: "{:<20}".format(str(_).strip())]), 'plain'))
        
        server = smtplib.SMTP(smtp_server, smpt_port)
        server.starttls()
        server.login(sender_email, sender_pwd)
        server.sendmail(sender_email, [email.strip() for email in recep_emails.split(";")], msg.as_string())
        print('ok')
        server.quit()

if __name__ == '__main__':

    ed = EthnicityDetector()
    vf = np.vectorize(ed.get_ethnicity)

    tc = TableHandler(**json.load(open("config/conn-02.ini", "r")))

    tc.proc_new_customers()

    print("identifying ethnicities in parallel...")

    AVAIL_CPUS = multiprocessing.cpu_count()

    print(f'available CPUs: {AVAIL_CPUS}')
    print('creating a pool...', end='')
    pool = multiprocessing.Pool(AVAIL_CPUS)
    print('ok')

    tc._detected_ethnicities = pd.DataFrame(np.vstack(pool.map(get_array_ethnicity, 
                                    np.array_split(tc._CUST_TO_CHECK.values, AVAIL_CPUS))),
                                columns=["CustomerID", "Ethnicity"], dtype=str).query('Ethnicity != "None"')

    print('detected:')
    print(tc._detected_ethnicities.head())

    pool.close()
    pool.join()
    
    print("found {} rows with some ethnicities".format(len(tc._detected_ethnicities)))
    print(tc._detected_ethnicities.head())

    tc.update_ethnicity_table()
    # tc.send_email()
