import pandas as pd
import json
import time
import arrow

from collections import defaultdict, Counter
from unidecode import unidecode

from string import ascii_lowercase

import sqlalchemy as sa
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy import text

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

        stk = np.hstack((b[:,0].reshape(b.shape[0],1), b[:,-1].reshape(b.shape[0],1) ,ets.reshape(b.shape[0],1)))
    
        return stk

class TableHandler(object):
    
    """
    Class to connect to tables and get or upload stuff from/to tables
    """
    def __init__(self, server, user, port, user_pwd, db_name, src_table, target_table, **kwargs):

        if 'days' in kwargs:
            self.DAYS = kwargs['days']
        elif 'years' in kwargs:
            self.DAYS = kwargs['years']*365
        else:
            print('you forgot to specify days or years!')
            sys.exit(0)

        
        self.SRC_TABLE = src_table   # where we take customer id and name from 
        self.TARGET_TABLE = target_table
        # self.DAYS = days

        self.CUSTID = 'CustomerID'

        self._SESSION = sessionmaker(autocommit=True)
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
        return self._ENGINE.execute(f'SELECT COUNT (*) FROM {tab};').fetchone()[0]

    def _make_value(self, value):

            if not isinstance(value, str):
                if not isinstance(value, int):
                    return 'NULL'
                else:
                    return str(value)

            _ = ''.join([v for v in value if v.isalnum() or v.isspace()])

            if len(_) > 0:
                return _
            else:
                return 'NULL'
                
    @timer
    def upload(self, tbl, target_tbl):

        """
        uploads **any** table tbl to a table target_tbl
        """

        # print('before upload to temp table starts..')
        # print(tbl[tbl['FullName'].apply(lambda x: 'Lu' == x.strip())])

        # sys.exit(0)

        # dtypes to SQL types dictionary
        dt_to_type = {'int64': 'int', 'object': 'nvarchar(200)'}

        try:
            self.sess.execute(f""" drop table {target_tbl} """)
        except:
            print(f'note: didn\'t drop {target_tbl} because it didn\'t exist')

        self.sess.execute(f""" create table {target_tbl} 
                                ({', '.join([c + ' ' + dt_to_type[tbl.dtypes[i].name] for i, c in enumerate(tbl.columns)])})

                             """)
        _MAX_ROWS = 1000

        # there's a limit of **1,000** rows per query when attempting to insert as below


        for i in range(divmod(len(tbl), _MAX_ROWS)[0] + 1):
            self.sess.execute(f""" 
                                INSERT INTO  {target_tbl} 
                                VALUES {', '.join(['(' + ','.join(["'" + self._make_value(v) + "'" for v in r[1].values]) + ')'  
                                                                            for r in tbl.iloc[_MAX_ROWS*i:_MAX_ROWS*(i+1)].iterrows()])}
                             """)

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

        print(f'total {NROWS_SRC} rows to analyze in {self.SRC_TABLE}')


        self._CUST_TO_CHECK = pd.read_sql(f"""
                                            SELECT {self.CUSTID},
                                            ISNULL(FirstName,'') + ' ' + ISNULL(MiddleName,'') + ' ' + ISNULL(LastName,'') as FullName
                                            FROM {self.SRC_TABLE} 
                                            WHERE {self.QRY_TIMESPAN}
                                            """,
                                            self._ENGINE)

        return self
    
    
    @timer
    def update_ethnicity_table(self):
         
        print("updating ethnicity table..")

        if len(self._detected_ethnicities) < 1:

            print("[WARNING]: no new ethnicities to upload!")

        else:

            if self.exists(self.TEMP_TABLE):
                print(f'table {self.TEMP_TABLE} exists and has {self.count_rows(self.TEMP_TABLE)} rows')
            else:
                print(f'table {self.TEMP_TABLE} doesn\'t exist')

            t_start = time.time()

            # print(f' now uploading {self._detected_ethnicities.head()} to {self.TEMP_TABLE}')
            # self._detected_ethnicities.to_sql(self.TEMP_TABLE, self._ENGINE, 
            #         if_exists='replace',  #  if exists, drop it, recreate and insert data
            #         index=False, 
            #         dtype={"CustomerID": sa.types.Integer, 
            #                 "Ethnicity": sa.types.String,
            #                 "AssignedOn": sa.types.String}, 
            #         chunksize=(None if len(self._detected_ethnicities) <= 1000 else 1000))

            self.upload(self._detected_ethnicities, self.TEMP_TABLE)

            print(f'AFTER update temp table has {self.count_rows(self.TEMP_TABLE)} rows')

            print('now starting target table update...')

            if self.exists(self.TARGET_TABLE):

                print(f'target table {self.TARGET_TABLE} exists and has {self.count_rows(self.TARGET_TABLE)} rows') 

                print(f"merging with {self.TEMP_TABLE}..")

                # self.sess.execute(text(f"""
                #                 MERGE {self.TARGET_TABLE} AS TARGET
                #                 USING {self.TEMP_TABLE} AS SOURCE
                #                 ON TARGET.CustomerID = SOURCE.CustomerID
                #                 WHEN MATCHED AND (TARGET.Ethnicity <> SOURCE.Ethnicity)
                #                 THEN
                #                 UPDATE
                #                 SET TARGET.Ethnicity = SOURCE.Ethnicity, TARGET.AssignedOn = SOURCE.AssignedOn
                #                 WHEN NOT MATCHED BY TARGET
                #                 THEN
                #                 INSERT (CustomerID, Ethnicity, AssignedOn)
                #                 VALUES (SOURCE.CustomerID, SOURCE.Ethnicity, SOURCE.AssignedOn);
                #                 """))

                self.sess.execute(f"""DELETE FROM {self.TARGET_TABLE}
                                        WHERE 
                                        {self.CUSTID} IN 
                                        (SELECT {self.CUSTID} FROM {self.TEMP_TABLE})""")
                print(f'deleted some customer ids from target, left rows: {self.count_rows(self.TARGET_TABLE)}')

                self.sess.execute(f"""
                            INSERT INTO {self.TARGET_TABLE} ({self.CUSTID}, FullName, Ethnicity, AssignedOn)
                            SELECT {self.CUSTID}, FullName, Ethnicity, AssignedOn 
                            FROM
                            {self.TEMP_TABLE};
                    """)

                print(f'target table {self.TARGET_TABLE} now has {self.count_rows(self.TARGET_TABLE)} rows')

            else:

                print(f'target table {self.TARGET_TABLE} doesn\'t exist. attempting to create...')

                self.sess.execute(f"""  CREATE TABLE {self.TARGET_TABLE} 
                                        ({self.CUSTID} int, FullName nvarchar(200), Ethnicity nvarchar(200), AssignedOn nvarchar(200))
                                    """)
                print(f'inserting new values into target table...')
                
                self.sess.execute(f"""  INSERT INTO {self.TARGET_TABLE} 
                                        ({self.CUSTID}, FullName, Ethnicity, AssignedOn) 
                                        SELECT {self.CUSTID}, FullName, Ethnicity , AssignedOn
                                        FROM {self.TEMP_TABLE}
                                    """)
                print(f'target table {self.TARGET_TABLE} now has {self.count_rows(self.TARGET_TABLE)} rows')
                  
            
            print("update completed. elapsed time: {:.0f} min {:.0f} sec".format(*divmod(time.time() - t_start, 60)))
    
    
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

    tc = TableHandler(**json.load(open("config/conn-02.ini", "r")), 
                        src_table='DWSales.dbo.tbl_LotusCustomer',
                        target_table='TEGA.dbo.CustomerEthnicities_20',
                        years=1)

    tc.proc_new_customers()

    AVAIL_CPUS = multiprocessing.cpu_count()

    print(f'available CPUs: {AVAIL_CPUS}. creating a pool...', end='')
    pool = multiprocessing.Pool(AVAIL_CPUS)
    print('ok')

    tc._detected_ethnicities = pd.DataFrame(np.vstack(pool.map(get_array_ethnicity, 
            np.array_split(tc._CUST_TO_CHECK.values, AVAIL_CPUS))),
                    columns=["CustomerID", "FullName", "Ethnicity"],
                    dtype=str).query('Ethnicity != "None"')

    pool.close()
    pool.join()

    tc._detected_ethnicities['CustomerID'] = tc._detected_ethnicities['CustomerID'].astype(int)
    tc._detected_ethnicities["AssignedOn"] = tc.TODAY_SYD

    # print(tc._detected_ethnicities.head())
    print(f'total rows with ethnicity: {len(tc._detected_ethnicities)}') 

    tc.update_ethnicity_table()
    tc.send_email()
