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


class TableHandler(object):
    
    """
    Class to connect to tables and get or upload stuff from/to tables
    """
    def __init__(self, src_conf_file = 'connection-loc.ini',
                        target_conf_file = 'connection.ini', 
                            src_table='[DWSales].[dbo].[tbl_LotusCustomer]', 
                                target_table='CustomerEthnicities'):
        
        self.SRC_TABLE = src_table    # normally [DWSales].[dbo].[tbl_LotusCustomer]
        self.TARGET_TABLE = target_table
        self.SRC_CONF_FILE = src_conf_file
        self.TARGET_CONF_FILE = target_conf_file
        self.LAST_SEVEN_DAYS = " ".join(['BETWEEN', "'" + (datetime.now() + timedelta(days=-6)).strftime("%Y%m%d") + "'", "AND", "'" +
        datetime.now().strftime("%Y%m%d") + "'"])
        
        self.qopts = {"last_7_days": """((([ModifiedDate] >= [CreatedDate]) AND ([ModifiedDate] {}))
                    OR ([CreatedDate] {}) ) AND ([CustomerListID] =
                    2)""".format(*[self.LAST_SEVEN_DAYS]*2), "before_today":
                    """((([ModifiedDate] >= [CreatedDate]) AND ([ModifiedDate]
                    <= {})) OR ([CreatedDate] <= {}) ) AND ([CustomerListID] =
                    2)""".format(*["'" + (datetime.now() +
                        timedelta(days=-1)).strftime("%Y%m%d") + "'"]*2)}
        self.timespan_descr = {"last_7_days": "within last seven days, " +  self.LAST_SEVEN_DAYS.lower(),
                "before_today": "before today"}

        self.TODAY_SYD = arrow.utcnow().to('Australia/Sydney').format('DD-MM-YYYY')
 
    # wrapper around pandas' to_sql
    def  _push_to_sql(self, df, tart_name, eng):
       
        df.to_sql(tart_name, eng, if_exists='append', index=False, dtype={"CustomerID": sa.types.String(length=20),
                                                                            "Ethnicity": sa.types.String(length=50),
                                                                            "AssignedOn": sa.types.String(length=10)},
                                                                            schema="TEGA.dbo",
                                                                            chunksize=200000)

    def connect(self, src_or_target='src'):
        
        CONF_FILE = self.SRC_CONF_FILE if src_or_target == 'src' else self.TARGET_CONF_FILE
        
        try:
            self.SERVER, self.USER, self.PORT, self.PWD, self.DRIVER, self.DB_NAME = [line.split("=")[-1].strip() 
                    for line in open("config/" + CONF_FILE, 'r').readlines() if line.strip()]
        except:
            print("[ERROR]: PROBLEM WITH CONFIGURATION FILE,  EXITING..")
            sys.exit(0)
        
        self.CONNSTR = 'mssql+pymssql://{}:{}@{}:{}/{}'.format(self.USER, self.PWD, self.SERVER, self.PORT, self.DB_NAME)
        #print("connection string: {}".format(self.CONNSTR))
    
        try:
            self._ENGINE = sa.create_engine(self.CONNSTR)
        except:
            print('[ERROR]: CAN\'T CREATE ENGINE, EXITING..')
            sys.exit(0)

        self._CONNECTION = self._ENGINE.connect()
        
        print('connected to {}'.format(self.SERVER))

        return self
    
    
    def disconnect(self):

        self._CONNECTION.close()
        print('disconnected from {}'.format(self.SERVER))


    def get_customers(self):
        
        """
        get all new customers of interest from Lotus and p[ut them into a data frame]
        """

        # first just get the number of interesting customers on Lotus
        NROWS_SRC = self._CONNECTION.execute("SELECT COUNT (*) FROM " + self.SRC_TABLE +  "WHERE " +
                self.qopts["last_7_days"]).fetchone()[0]

        print('there are {} rows to analyse in {}...'.format(NROWS_SRC, self.SRC_TABLE))
        
        # now download interesting customers: we only need their customer id (cust_id)
        # and full name which is to be cmoposed from their first, last and amiddle names

        self.TODAYS_CUSTOMERS = pd.DataFrame()

        # reading by chunks
        print("collect the customers of interest...")

        for b in pd.read_sql("SELECT [CustomerID] as [cust_id], "
                             "RTRIM(LTRIM(LOWER( ISNULL([FirstName],'') + ' ' + ISNULL([MiddleName],'') + ' ' + ISNULL([LastName],'') ))) as [full_name] "
                            "FROM " + self.SRC_TABLE + " WHERE " + self.qopts["last_7_days"], self._ENGINE,
                                    chunksize=200000):

            self.TODAYS_CUSTOMERS = pd.concat([self.TODAYS_CUSTOMERS, b])
        
        print("collected {} customers from {}...".format(len(self.TODAYS_CUSTOMERS), self.SRC_TABLE))
            
        return self
    
    def upload_ethnicities(self, df_ethn):
         
        """

        IN: df_ethn - data frame with newly detected customer ethnicities; columns are CustomerID and Ethnicity 
        OUT: nothing

        Upload df_ethn to the ethnicity table; if some customer ids are already there possibly with different ethnicities assigned, 
        OVERWHITE

        """
        print("starting to upload ethnicities...")

        if len(df_ethn) < 1:

            print("no new ethnicities to upload")

        else:

            # get customer ids (as strings) that are already in the ethnicity table
            try:
                self.IDS_IN_TABLE = set(pd.read_sql("SELECT [CustomerID] FROM " + self.TARGET_TABLE + ";", 
                                                self._ENGINE)["CustomerID"].astype(str))
            except:
                 self.IDS_IN_TABLE = set()
            
            print('customer ids already in {}: {}...'.format(self.TARGET_TABLE, len(self.IDS_IN_TABLE)))
    
            # any  customer ids we figured out ethnicity for already in the ethnicity table?
            # recall that we need to replace those with new ethnicity information
            self.IDS_TO_REPLACE = self.IDS_IN_TABLE & set(df_ethn.CustomerID)
            print("need to replace {} of these".format(len(self.IDS_TO_REPLACE)))

            if self.IDS_TO_REPLACE:

                # since it's possible that we'll have to delete rows for many thouthands of 
                # customer ids, we need to pypass the SQL limitation on th number of items on
                # a list in say WHERE x IN (...); we put the ids to delete in a temporary table
                # frist and then delete...

                print("writing customer ids to be REPLACED into a temporary table - expect {} ids to end up there..".format(len(self.IDS_TO_REPLACE)))

                pd.DataFrame({"CustomerID":
                    list(self.IDS_TO_REPLACE)}).to_sql("ethtmp", self._ENGINE, if_exists='replace', index=False, 
                    dtype={"CustomerID": sa.types.String(length=20)}, schema="TEGA.dbo", chunksize=200000)

                ROWS_TMP = self._CONNECTION.execute("SELECT COUNT (*) FROM TEGA.dbo.ethtmp").fetchone()[0]
                
                if ROWS_TMP:
                    print("created a temporary table [TEGA].[dbo].ethtmp with {} rows...".format(ROWS_TMP))
                else:
                    print("[ERROR]: SOMETHING IS WRONG WITH TEMP TABLE - IT SEEMS EMPTY!")

                print("attempting to delete these rows from {}...".format(self.TARGET_TABLE))
                self._CONNECTION.execute("DELETE FROM " + self.TARGET_TABLE + " WHERE " + "CustomerID in (SELECT CustomerID FROM TEGA.dbo.ethtmp);")
                print("expected rows left in the table: {}".format(len(self.IDS_IN_TABLE) -len(self.IDS_TO_REPLACE)))
                
            # new customer ids to be appended to the ethnicity table
            print("starting appending new rows to {}...".format(self.TARGET_TABLE))
            
            self.IDS_TO_APPEND = set(df_ethn.CustomerID)
    
            if self.IDS_TO_APPEND:

                print("customer ids to append: {}".format(len(self.IDS_TO_APPEND)))
                
                df_ethn["AssignedOn"] = self.TODAY_SYD

                self._push_to_sql(df_ethn.loc[df_ethn.CustomerID.isin(self.IDS_TO_APPEND),['CustomerID',
                    'Ethnicity', 'AssignedOn']], 
                    self.TARGET_TABLE, self._ENGINE)
    
                print("ok")
            else:
                print("nothing to upload to {}".format(self.TARGET_TABLE))
    
    
    def send_email(self, df_ethn):
        
        sender_email, sender_pwd, smtp_server, smpt_port, recep_emails = [line.split("=")[-1].strip() 
                                    for line in open("config/email.cnf", "r").readlines() if line.strip()]
        
        msg = MIMEMultipart()   
        
        msg['From'] = sender_email
        msg['To'] = recep_emails
        msg['Subject'] = 'update on ethnicities: customers created or modified {}'.format(self.timespan_descr["last_7_days"])
        
        dsample = pd.DataFrame()

        for k, v in Counter(df_ethn['Ethnicity']).items():
            this_ethnicity = df_ethn[df_ethn.Ethnicity == k]
            ns = 3 if len(this_ethnicity) > 2 else 1
            dsample = pd.concat([dsample, this_ethnicity.sample(n=ns)])

        st_summary  = "-- new ethnic customer ids captured:\n\n" + \
                "".join(["{}: {}\n".format(ks.upper(), vs) for ks, vs in sorted([(k,v) 
                    for k, v in Counter(df_ethn['Ethnicity']).items()], key=lambda x: x[1], reverse=True)])
        
        msg.attach(MIMEText(st_summary+ "\n-- sample:\n\n" + dsample.loc[:,["CustomerID", "FullName", "Ethnicity"]].to_string(index=False, justify="left",
            formatters=[lambda _: "{:<12}".format(str(_).strip()), lambda _: "{:<30}".format(str(_).strip()), lambda _: "{:<20}".format(str(_).strip())]), 'plain'))
        server = smtplib.SMTP(smtp_server, smpt_port)
        server.starttls()
        print('sending email notification...',end='')
        server.login(sender_email, sender_pwd)
        server.sendmail(sender_email, [email.strip() for email in recep_emails.split(";")], msg.as_string())
        print('ok')
        server.quit()

class EthnicityDetector(object):
    
    def __init__(self, df, ethnicity_list):
        
        self.DATA_DIR = "/home/igork/data/"
        self.NAME_DATA_DIR = self.DATA_DIR + "names/"
        self.ethnicity_list = ethnicity_list
        self.input_df = df
        
        # load name and surname databases
        self.name_dict = json.load(open(self.NAME_DATA_DIR + "names_26092017.json", "r"))
        self.surname_dict = json.load(open(self.NAME_DATA_DIR + "surnames_26092017.json", "r"))
        self.names_international = {line.strip().lower() for line in open(self.NAME_DATA_DIR + "names_international.txt", "r").readlines() if line}
        self.surname_ending_dict = json.load(open(self.NAME_DATA_DIR + "surname_endings_06102017.json", "r"))
        self.deciders = {"arabic": "name_and_surname", "italian": "name_and_surname", 
                         "filipino": "name_and_surname", "indian": "name_or_surname",
                         "japanese": "name_and_surname", "serbian": "surname", "thai": "name", "vietnamese": "surname"}
        
        # make name and surname dictionaries by letter for required ethnicities
        self.names = defaultdict(lambda: defaultdict(set))
        self.surnames = defaultdict(lambda: defaultdict(set))

        self.CHUNK_SIZE = 10000    # in rows
    
    def __create_ethnic_dicts(self):
        
        for ethnicity in self.ethnicity_list:
            
            if ethnicity in self.name_dict:
                self.names[ethnicity] = {letter: {unidecode(w["name"]) for w in self.name_dict[ethnicity] 
                                                 if w["name"].isalpha() and unidecode(w["name"][0]) == letter} for letter in ascii_lowercase}
                
            if ethnicity in self.surname_dict:
                self.surnames[ethnicity] = {letter: {unidecode(w) for w in self.surname_dict[ethnicity] 
                                                 if w.isalpha() and unidecode(w)[0] == letter} for letter in ascii_lowercase}
                
        return self
    
    def clean_input(self):
        
        self.input_df.dropna(inplace=True)

        # replace separators with white spaces, then make sure there's only 1 white space is separating name parts
        self.input_df.loc[:, "full_name"] = (self.input_df["full_name"].apply(unidecode)
                                                                .str.replace("'", " ").str.replace("-", " ")
                                                                .str.replace("(", " ").str.replace(")", " ")
                                                                .str.split().str.join(' ').str.lower())

        # only keep names that have parts consisting of letters and longer than a single character; places '' where no proper full names
        self.input_df.loc[:, "full_name"] = self.input_df["full_name"].apply(lambda _: " ".join([p for p in _.split() 
                                                                                                 if p.isalpha() and len(p) > 1])).str.strip()
        self.input_df = self.input_df.loc[self.input_df["full_name"].apply(lambda x: isinstance(x, str) and len(str(x).strip()) > 0), :]
        
        return self
    
    def __search_surname(self, st):
        
        exmtch_s_ethns = set()
        
        for ethnicity in self.ethnicity_list:
            if ethnicity in self.surnames:
                for name_prt in st.split():
                    if name_prt in self.surnames[ethnicity][name_prt[0]]:
                        exmtch_s_ethns.add(ethnicity) 
        
        if (not exmtch_s_ethns):
            for ethnicity in self.ethnicity_list:
                if ethnicity in self.surname_ending_dict:
                    for ending in self.surname_ending_dict[ethnicity]:
                        for name_prt in st.split():
                            if name_prt.endswith(ending) and (len(name_prt) - len(ending) > 1):
                                exmtch_s_ethns.add(ethnicity)
        
        return "|".join(sorted(list(exmtch_s_ethns)))
            
    
    def find_ethnicity_candidates(self):
   
        self.__create_ethnic_dicts()
    
        self.input_df.loc[:, "n_ethn"] = self.input_df["full_name"].apply(lambda _: "|".join([ethnicity for ethnicity in self.ethnicity_list if 1 in 
                                                                      [1 for name_prt in _.split() if ((name_prt not in self.names_international) and 
                                                                        (name_prt in self.names[ethnicity][name_prt[0]]))]]))
        
        self.input_df.loc[:, "s_ethn"] = self.input_df["full_name"].apply(self.__search_surname)
             
        return self 
    
    def pick_ethnicity(self):
        
        self.input_df = self.input_df[(self.input_df["n_ethn"].str.len() > 1) | (self.input_df["s_ethn"].str.len() > 1)]
        
        final_ethnicities = defaultdict(lambda: defaultdict(str))
        
        for row in self.input_df.iterrows():
            
            customer_ethnicities = set()
            
            for ethnicity in self.ethnicity_list:

                if self.deciders[ethnicity] in ["name", "name_or_surname"]:
                    # if there's an ethnicity candidate based on name, add it
                    # to the final ethnicity set
                    if ethnicity in row[1].n_ethn:
                        customer_ethnicities.add(ethnicity)
                elif self.deciders[ethnicity] in ["surname", "name_or_surname"]:
                    if ethnicity in row[1].s_ethn:
                        customer_ethnicities.add(ethnicity)
                elif self.deciders[ethnicity] == "name_and_surname":
                    if (ethnicity in row[1].s_ethn) and (ethnicity in row[1].n_ethn):
                        customer_ethnicities.add(ethnicity)
            # customer_ethnicities = [self.ethn_abbrs[e] for e in customer_ethnicities]
            final_ethnicities[row[1].cust_id] = {"FullName": row[1].full_name,
                    "Ethnicity" : "|".join(sorted(list(customer_ethnicities))) if customer_ethnicities else None}
        
        self._detected_ethnicities = pd.DataFrame.from_dict(final_ethnicities, orient='index').reset_index().rename(columns={"index": "CustomerID"}).dropna()
        self._detected_ethnicities['CustomerID'] = self._detected_ethnicities['CustomerID'].astype(str)
        print("total new customer ids we detected ethnicity for: {}".format(len(self._detected_ethnicities.CustomerID)))

        return self

if __name__ == '__main__':

    tc = TableHandler()

    def job():
        
        print('/njob started at {}...'.format(datetime.now()))

        t0 = time.time()
        
        tc.connect('src')
        tc.get_customers()
        tc.disconnect()
        
        if len(tc.TODAYS_CUSTOMERS):

            ed = EthnicityDetector(tc.TODAYS_CUSTOMERS, 
                ["indian", "filipino", "japanese", "arabic", "italian", "serbian", "thai", "vietnamese"]).clean_input()
            ed.find_ethnicity_candidates().pick_ethnicity()
             
             
            tc.connect('src')
            tc.upload_ethnicities(ed._detected_ethnicities)
            tc.disconnect()
        
        tc.send_email(ed._detected_ethnicities)

        print("elapsed time: {:.0f} minutes {:.0f} seconds".format(*divmod(time.time() - t0, 60)))

    schedule.every().day.at('22:00').do(job)
    
    while True:
        schedule.run_pending()
