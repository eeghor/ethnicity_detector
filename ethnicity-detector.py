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

def timer(func):
    def wrapper(*args, **kwargs):
        t_start = time.time()
        res = func(*args, **kwargs)
        print("f: {} # elapsed time: {:.0f} m {:.0f}s".format(func.__name__.upper(), *divmod(time.time() - t_start, 60)))
        return res
    return wrapper

def complain_and_exit(txt):
    print(txt)
    sys.exit(0)

def get_array_ethnicity(b):  

        """
        input: b is a numpy array that has two columns: customer id and full name
        
        returns: numpy array with 3 columns: customer id, full name and ethnicity
        
        note that there will be 'None' where no ethnicity has been detected 
        """
    
        ets = vf(b[:,-1])  # we assume that the second column contains full names

        stk = np.hstack((b[:,0].reshape(b.shape[0],1), 
                        b[:,-1].reshape(b.shape[0],1), 
                        ets.reshape(b.shape[0],1)))
    
        return stk

class TableHandler(object):
    
    """
    Class to connect to tables and get or upload stuff from/to tables
    """
    def __init__(self, server, user, port, user_pwd, db_name, src_table, 
        target_table, temp_table='TEGA.dbo.tempo_LARGE_table', 
        email=None, **kwargs):

        if 'days' in kwargs:
            self.DAYS = kwargs['days']
        elif 'years' in kwargs:
            self.DAYS = int(round(kwargs['years']*365,0))
        else:
           complain_and_exit('you must specify days or years!') 

        
        self.SRC_TABLE = src_table   # where we take customer id and name from 
        self.TARGET_TABLE = target_table
        self.TEMP_TABLE = temp_table

        self.S3_CREDENTIALS = json.load(open('config/creds-s3.json', 'r'))

        self.CUSTID = 'CustomerID'

        # initiate session
        self._SESSION = sessionmaker(autocommit=True)
        self._ENGINE = sa.create_engine(f'mssql+pymssql://{user}:{user_pwd}@{server}:{port}/{db_name}')
        self._SESSION.configure(bind=self._ENGINE)
        self.sess = self._SESSION()

        # today as a time stamp
        _today_ts = arrow.utcnow().to('Australia/Sydney')
        # today as a string
        self.TODAY_SYD = _today_ts.format('DD-MM-YYYY')

        _days_ago_ts = _today_ts.shift(days=-self.DAYS)

        self.BETWEEN_DAYS = f'BETWEEN \'{_days_ago_ts.format("YYYYMMDD")}\' AND \'{_today_ts.format("YYYYMMDD")}\''
        
        # finally, an empty data frame to store detected ethnicities
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
        return self.sess.execute(f'SELECT COUNT (*) FROM {tab};').fetchone()[0]

    def _make_value(self, value):

            if not isinstance(value, str):
                if not isinstance(value, int):
                    return 'NULL'
                else:
                    return str(value)

            _ = ''.join([v for v in value if v.isalnum() or (v in {'-','|',' '})]).lower()

            if len(_) > 0:
                return _
            else:
                return 'NULL'

    def _column_type(self, s, types, examp_tab):

            """
            s is a series we want to figure out the corresponding SQL Server type for
            """

            _defaults = {'int64': 'int NOT NULL',
                         'object': 'varchar(200)'}

            series_type = str(s.dtypes)
            series_name = s.name

            if types == 'defaults':
                
                try:
                    return _defaults[series_type]
                except:
                    # apparently, series type is not int or string
                    complain_and_exit(f'can\'t figure out column type for {series_name}!')

            if types == 'copy':

                if not examp_tab:
                    complain_and_exit(f'you forgot to provide an example table to copy column types from!')


                TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME = [_.replace('[','').replace(']','').strip() 
                                                            for _ in examp_tab.split('.')]

                """
                table information is supposed to be a table with the following columns:
                TABLE_CATALOG | TABLE_SCHEMA | TABLE_NAME | COLUMN_NAME | ORDINAL_POSITION | COLUMN_DEFAULT
                IS_NULLABLE | DATA_TYPE | CHARACTER_MAXIMUM_LENGTH
                so below we expect to get something like [('int', None, 'YES')]:
                """

                try:
                    inf_ = self.sess.execute(f""" SELECT DATA_TYPE, 
                                                         CHARACTER_MAXIMUM_LENGTH, 
                                                         IS_NULLABLE 
                                                  FROM INFORMATION_SCHEMA.COLUMNS
                                                  WHERE
                                                  TABLE_CATALOG='{TABLE_CATALOG}' AND 
                                                  TABLE_SCHEMA = '{TABLE_SCHEMA}' AND 
                                                  TABLE_NAME = '{TABLE_NAME}' AND
                                                  COLUMN_NAME = '{s.name}';""").fetchall().pop()
                except:
                    print(f'problem obtaining table info for {examp_tab}')
                    sys.exit(0)

                return ' '.join([inf_[0], 
                                '' if not inf_[1] else '(' + str(inf_[1]) + ')', 
                                '' if inf_[2].lower() == 'yes' else 'NOT NULL']).strip()
                

            if types == 'optimal':

                _lengths = [20, 50, 200]

                if series_type == 'int64':
                    return 'int'

                elif series_type == 'object':
                    max_len = len(s.max())
                    return f'varchar({min(_lengths, key=lambda x: x - max_len if x - max_len > 0 else 1000)})'
                else:
                    complain_and_exit(f'can\'t figure out column type for {series_name}!')

                
    @timer
    def upload_to_temporary_table(self, tbl, target_tbl):

        """
        uploads table tbl to a temporary table target_tbl
        """

        # does the target table even exist? if it does, drop it

        if self.exists(target_tbl):
            self.sess.execute(f'drop table {target_tbl}')
        else:
            print(f'target temporary table {target_tbl} doesn\'t exist')
        
        # now create new temporary table

        self.sess.execute(f""" CREATE TABLE {target_tbl} 
                                ({', '.join([c + ' ' + self._column_type(tbl[c], types='copy', examp_tab=self.TARGET_TABLE) 
                                                        for c in tbl.columns])})

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
    def get_new_rows(self):
        
        """
        get all new customers of interest from Lotus and put them into a data frame
        """

        self._CUST_TO_CHECK = pd.read_sql(f"""
                                            SELECT {self.CUSTID},
                                            ISNULL(FirstName,'') + ' ' + ISNULL(MiddleName,'') + ' ' + ISNULL(LastName,'') as FullName
                                            FROM {self.SRC_TABLE} 
                                            WHERE ({self.CUSTID} IS NOT NULL) AND 
                                            (
                                                ((ModifiedDate >= CreatedDate) AND (ModifiedDate {self.BETWEEN_DAYS}))
                                                OR
                                                (CreatedDate {self.BETWEEN_DAYS})

                                            )
                                            AND (CustomerListID = 2)
                                            """,
                                            self._ENGINE)

        print(f'now rows: {len(self._CUST_TO_CHECK)}')

        return self
    
    
    @timer
    def update_ethnicity_table(self):
         
        print("updating ethnicity table..")

        if len(self._detected_ethnicities) == 0:

            print("[WARNING]: no new ethnicities to upload!")

        else:

            self.upload_to_temporary_table(self._detected_ethnicities, self.TEMP_TABLE)

            if self.exists(self.TARGET_TABLE):

                print(f'currently, table {self.TARGET_TABLE} has {self.count_rows(self.TARGET_TABLE)} rows') 

                self.sess.execute(f"""DELETE FROM {self.TARGET_TABLE}
                                        WHERE 
                                        {self.CUSTID} IN 
                                        (SELECT {self.CUSTID} FROM {self.TEMP_TABLE})""")

                print(f'rows to remain unchanged: {self.count_rows(self.TARGET_TABLE)}') 

                self.sess.execute(f"""
                            INSERT INTO {self.TARGET_TABLE} ({self.CUSTID}, FullName, Ethnicity, AssignedOn)
                            SELECT {self.CUSTID}, FullName, Ethnicity, AssignedOn 
                            FROM
                            {self.TEMP_TABLE};
                            """)

                print(f'rows after update: {self.count_rows(self.TARGET_TABLE)}') 

            else:

                print(f'target table {self.TARGET_TABLE} doesn\'t exist. attempting to create...')

                self.sess.execute(f"""  CREATE TABLE {self.TARGET_TABLE} 
                                        ({self.CUSTID} int, FullName varchar(100), Ethnicity varchar(50), AssignedOn nvarchar(20))
                                    """)
                print(f'inserting new values into target table...')
                
                self.sess.execute(f"""  INSERT INTO {self.TARGET_TABLE} 
                                        ({self.CUSTID}, FullName, Ethnicity, AssignedOn) 
                                        SELECT {self.CUSTID}, FullName, Ethnicity , AssignedOn
                                        FROM {self.TEMP_TABLE}
                                    """)
                print(f'target table {self.TARGET_TABLE} now has {self.count_rows(self.TARGET_TABLE)} rows')
                  
    
    def send_email_sns(self):

        self.TOPIC_ARN = [l.strip() for l in open('config/arn.txt','r').readlines() if l.strip()].pop()

        boto3.client('sns', **self.S3_CREDENTIALS).publish(TopicArn=self.TOPIC_ARN,
                                                Subject='new ethnicities', 
                                                    Message='Today\'s ethnicity update: we have identified {} customer IDs with ethnic names!\nThis is for the last {} days.\nNot bad.'.format(len(self._detected_ethnicities), self.DAYS))


    def send_email(self):

        print("sending email notification...", end='')
        
        sender_email, sender_pwd, smtp_server, smpt_port, recep_emails = [line.split("=")[-1].strip() 
                                    for line in open("config/email.cnf", "r").readlines() if line.strip()]
        
        msg = MIMEMultipart()   
        
        msg['From'] = sender_email
        msg['To'] = recep_emails
        msg['Subject'] = f'ethnicity update: {len(self._detected_ethnicities)} new {self.BETWEEN_DAYS.lower()}'
        
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
            print('prepared text')
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


    def send_email_jinja(self):

        
        d = defaultdict()

        for i, t in enumerate(Counter(self._detected_ethnicities['Ethnicity']).most_common()[:3], 1):
            d[f'eth{i}'] = t[0]
            d[f'eth{i}_n'] = f'{t[1]:,}'


        sender_email, sender_pwd, smtp_server, smpt_port, recep_emails = [line.split("=")[-1].strip() 
                                    for line in open("config/email.cnf", "r").readlines() if line.strip()]
        
        email = MIMEMultipart('related')   
        
        email['From'] = sender_email
        email['To'] = recep_emails

        email['Subject'] = f"[ethnicity update] {len(self._detected_ethnicities):,} new between {arrow.utcnow().to('Australia/Sydney').shift(days=-self.DAYS).format('DD/MM/YYYY')} and {arrow.utcnow().to('Australia/Sydney').format('DD/MM/YYYY')}"
        

        file_loader = FileSystemLoader('templates')
        env = Environment(loader=file_loader)
        template = env.get_template('ethnicity_template.html')

        msg_text = template.render(**d, eth_tab=self.TARGET_TABLE)

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
        server.login(sender_email, sender_pwd)
        server.sendmail(sender_email, [email.strip() for email in recep_emails.split(";")], email.as_string())

        server.quit()


if __name__ == '__main__':

    ed = EthnicityDetector()
    vf = np.vectorize(ed.get_ethnicity)

    tc = TableHandler(**json.load(open("config/conn-02.ini", "r")), 
                        src_table='DWSales.dbo.tbl_LotusCustomer',
                        target_table='TEGA.dbo.CustomerEthnicities',
                        temp_table='TEGA.dbo.some_temptable',
                        days=1)

    tc.get_new_rows()

    ncust = len(tc._CUST_TO_CHECK)

    if ncust:
        # setup multiprocessing
        AVAIL_CPUS = multiprocessing.cpu_count()
        print(f'available CPUs: {AVAIL_CPUS}')        
    
        if ncust > 9000000:
    
            chunk_size = 1000000
    
            num_chunks, extra = divmod(ncust, chunk_size)
    
            print(f'full chunks: {num_chunks}, extra rows: {extra}')
    
            dfs = []
    
            for start in range(0, num_chunks + (extra > 0)):
    
                from_ = start*chunk_size
                to_ = from_ + chunk_size
    
                print(f'rows {from_} to {to_}...')
    
                pool = multiprocessing.Pool(AVAIL_CPUS)
    
                b = tc._CUST_TO_CHECK.iloc[from_:to_,:]
    
                res = np.vstack(pool.map(get_array_ethnicity, 
                                        np.array_split(b.values, AVAIL_CPUS)))
    
                pool.close()
                pool.join()
    
                dfs.append(pd.DataFrame(res,
                                    columns=["CustomerID", "FullName", "Ethnicity"],
                                    dtype=str).query('Ethnicity != "None"'))
    
            tc._detected_ethnicities = pd.concat(dfs)
    
        else:
    
            pool = multiprocessing.Pool(AVAIL_CPUS)
    
            tc._detected_ethnicities = pd.DataFrame(np.vstack(pool.map(get_array_ethnicity, 
                np.array_split(tc._CUST_TO_CHECK.values, AVAIL_CPUS))),
                        columns=["CustomerID", "FullName", "Ethnicity"],
                        dtype=str).query('Ethnicity != "None"')
    
            pool.close()
            pool.join()
    
    
        # convert CustomerID to int
        tc._detected_ethnicities['CustomerID'] = tc._detected_ethnicities['CustomerID'].astype(int)
        # add timestamp
        tc._detected_ethnicities["AssignedOn"] = tc.TODAY_SYD
    
        print(f'ethnic rows: {len(tc._detected_ethnicities)}') 

        tc.update_ethnicity_table()

        tc.send_email_jinja()

    else:
        print('no new customers today, nothing to do..')
