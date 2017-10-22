import pandas as pd
import json
import sys
from datetime import datetime, timedelta
from collections import defaultdict, Counter
from unidecode import unidecode
from string import ascii_lowercase
import sqlalchemy as sa

# for sending an email notification:
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import schedule
import time

#import pyodbc
import pymssql
import arrow

from ethnicitydetector import EthnicityDetector


class TableHandler(object):
    
    """
    Class to connect to tables and get or upload stuff from/to tables
    """
    def __init__(self, server, user, port, user_pwd, db_name,
                    src_table='[DWSales].[dbo].[tbl_LotusCustomer]', 
                        target_table='CustomerEthnicities', chsize=20000):
        
        self.SRC_TABLE = src_table   
        self.CHSIZE = chsize

        self._ENGINE = sa.create_engine('mssql+pymssql://{}:{}@{}:{}/{}'.format(user, user_pwd, server, port, db_name))
        
        self.LAST_SEVEN_DAYS = " ".join(['BETWEEN', "'" + (datetime.now() + timedelta(days=-6)).strftime("%Y%m%d") + "'", "AND", "'" +
        datetime.now().strftime("%Y%m%d") + "'"])
        
        self.QRY_TIMESPAN = {"last_7_days": """((([ModifiedDate] >= [CreatedDate]) AND ([ModifiedDate] {}))
                    OR ([CreatedDate] {}) ) AND ([CustomerListID] = 2)""".format(*[self.LAST_SEVEN_DAYS]*2), 
                    "before_today":
                    """((([ModifiedDate] >= [CreatedDate]) AND ([ModifiedDate]
                    <= {})) OR ([CreatedDate] <= {}) ) AND ([CustomerListID] = 2)""".format(*["'" + (datetime.now() +
                        timedelta(days=-1)).strftime("%Y%m%d") + "'"]*2)}

        self.timespan_descr = {"last_7_days": "within last seven days, " + self.LAST_SEVEN_DAYS.lower(),
                "before_today": "before today"}

        self.TODAY_SYD = arrow.utcnow().to('Australia/Sydney').format('DD-MM-YYYY')

        self._detected_ethnicities = pd.DataFrame()

        self.TEMP_TABLE = "tmptable"

        ed = EthnicityDetector()
 
    # wrapper around pandas' to_sql
    def  _push_to_sql(self, df_upload, into_table, eng):
       
        df_upload.to_sql(into_table, eng, if_exists='append', index=False, dtype={"CustomerID": sa.types.String(length=20),
                                                                            "Ethnicity": sa.types.String(length=50),
                                                                            "AssignedOn": sa.types.String(length=10)},
                                                                            schema="TEGA.dbo",
                                                                            chunksize=(None if len(df_upload) <= self.CHSIZE else self.CHSIZE))

    def proc_new_customers(self):
        
        """
        get all new customers of interest from Lotus and p[ut them into a data frame]
        """

        # first just get the number of interesting customers on Lotus
        NROWS_SRC = self._ENGINE.execute(" ".join(["SELECT COUNT (*) FROM", self.SRC_TABLE , "WHERE",
                                                        self.QRY_TIMESPAN["last_7_days"]])).fetchone()[0]

        print('there are {} rows to analyse in {}...'.format(NROWS_SRC, self.SRC_TABLE))

        t_start = time.time()

        for i, b in enumerate(pd.read_sql("SELECT [CustomerID], "
                             "ISNULL([FirstName],'') + ' ' + ISNULL([MiddleName],'') + ' ' + ISNULL([LastName],'') as [full_name] "
                            "FROM " + self.SRC_TABLE + " WHERE " + self.QRY_TIMESPAN["last_7_days"], 
                            self._ENGINE, chunksize=min(NROWS_SRC, self.CHSIZE)), 1):

            print("processing chunk #{} ({} rows)...".format(i, len(b)))
            b["Ethnicity"] = b.full_name.apply(ed.get_ethnicity)

            self._detected_ethnicities = pd.concat([self._detected_ethnicities, b[d.Ethnicity.notnull(), ["CustomerID", "Ethnicity"]]])
        
        print("done. time: {:.0f} min {:.0f} sec".format(*divmod(time.time() - t_start, 60)))
        print("found {} rows with some ethnicities".format(len(self._detected_ethnicities)))

        return self
    
    def update_ethnicity_table(self):
         
        print("updating ethnicity table..")

        if len(self._detected_ethnicities) < 1:
            print("[WARNING]: no new ethnicities to upload!")
        else:

            t_start = time.time()
            # add timestamp
            self._detected_ethnicities["AssignedOn"] = self.TODAY_SYD

            # upload all detected ethnicities into a temporary table
            self._detected_ethnicities.to_sql(self.TEMP_TABLE, self._ENGINE, 
                if_exists='replace', index=False, 
                    dtype={"CustomerID": sa.types.String(length=20)}, 
                    schema="TEGA.dbo", chunksize=(None if len(self._detected_ethnicities) <= self.CHSIZE else self.CHSIZE))

            ROWS_TMP = self._CONNECTION.execute("SELECT COUNT (*) FROM {};".format(self.TEMP_TABLE)).fetchone()[0]
                
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
        msg['Subject'] = 'ethnicities: customers created or modified {}'.format(self.timespan_descr["last_7_days"])
        
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
