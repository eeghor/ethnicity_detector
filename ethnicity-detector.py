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


class TableHandler(object):
    
    def __init__(self, driver='pyodbc', src_conf_file = 'connection-loc.ini',
            target_conf_file = 'connection.ini', 
            src_table='[DWSales].[dbo].[tbl_LotusCustomer]', target_table='CustomerEthnicities'):
        
        self.DRIVER_NAME = driver
        self.SRC_TABLE = src_table
        self.TARGET_TABLE = target_table
        self.SRC_CONF_FILE = src_conf_file
        self.TARGET_CONF_FILE = target_conf_file
        
        assert self.DRIVER_NAME in ['pyodbc', 'pymssql'], "wrong driver name - choose pyodbc or pymssql"
            
    def  _push_to_sql(self, df, tar, eng):
       
        df.to_sql(tar, eng, if_exists='append', index=False, dtype={"CustomerID": sa.types.String(length=20),
                                                                              "Ethnicity": sa.types.String(length=50)}, schema="TEGA.dbo")

    def connect(self, src_or_target='src'):
        
        CONF_FILE = self.SRC_CONF_FILE if src_or_target == 'src' else self.TARGET_CONF_FILE
        
        try:
            self.SERVER, self.USER, self.PORT, self.PWD, self.DRIVER, self.DB_NAME = [line.split("=")[-1].strip() 
                    for line in open("config/" + CONF_FILE, 'r').readlines() if line.strip()]
        except:
            print("problem with configuration file,  exiting..")
            sys.exit(0)

        self.CONNSTR = 'mssql+{}://{}:{}@{}:{}/{}'.format(self.DRIVER_NAME, self.USER, self.PWD, 
                                                          self.SERVER, self.PORT, self.DB_NAME)
        
        if self.DRIVER_NAME == 'pyodbc':
            import pyodbc
            self.CONNSTR = 'mssql+{}://{}/{}'.format(self.DRIVER_NAME, self.SERVER, self.DB_NAME)

            self.CONNSTR += '?driver=' + self.DRIVER
        elif self.DRIVER_NAME == 'pymssql':
            import pymssql
            
        try:
            self._ENGINE = sa.create_engine(self.CONNSTR)
        except:
            print('can\'t create engine, exiting..')
            sys.exit(0)

        self._CONNECTION = self._ENGINE.connect()
        
        print('connected to {}'.format(self.SERVER))

        return self
    
    
    def disconnect(self):

        self._CONNECTION.close()
        print('disconnected from {}'.format(self.SERVER))
    

    def get_customers(self, mx_chunk=100000):
        
        self.TODAY = (datetime.now() + timedelta(days=-1)).strftime("%Y%m%d")

        self.TODAY = '20140101'
        # how many rows are there in the Lotus table?
        nrows_qry = "SELECT COUNT (*) FROM " + self.SRC_TABLE + """ WHERE ([CustomerListID] = 2)
                             and ((([ModifiedDate] >= [CreatedDate]) and
                             ([ModifiedDate] <= '""" + self.TODAY + "')) or ([CreatedDate] <= '" + self.TODAY + "'));"
        
        NROWS_SRC = self._CONNECTION.execute(nrows_qry).fetchone()[0]
        print('there are {} rows of interest in the source table'.format(NROWS_SRC))
        
        if NROWS_SRC > mx_chunk:
            
            print('need to get customer data by chunks')

            cids_qry = "SELECT DISTINCT CustomerID FROM " + self.SRC_TABLE + """ WHERE
              ([CustomerListID] = 2) and ((([ModifiedDate] >= [CreatedDate]) and  ([ModifiedDate] <='""" + self.TODAY + "') or ([CreatedDate] <='" + self.TODAY + "'));"

            CIDS_SRC = sorted([row["CustomerID"] for row in self._CONNECTION.execute(cids_qry)])
            
            nchunks = NROWS_SRC if NROWS_SRC%mx_chunk == 0 else NROWS_SRC//mx_chunk + 1

            print('there will be {} chunks {} rows each'.format(nchunks, mx_chunk))

            for ch in range(nchunks):
                
                print('processing chunk {}..'.format(ch + 1))
                # create a temporary table with the customer ids to update
                pd.DataFrame({"CustomerID": [cid for cid in
                    CIDS_SRC[ch*mx_chunk:(ch+1)*mx_chunk]]}).to_sql('#TempIdTable' + str(ch), self._ENGINE, if_exists="replace", dtype={"CustomerID":  sa.types.String(length=20)}, schema='TEGA.dbo')
                #df_to_update = pd.read_sql("SELECT * FROM " + self.TARGET_TABLE + " where CustomerID in (SELECT CustomerID FROM #TempIdTable);", self._ENGINE)


        # get customer IDS from Lotus
        pick_customers_qry = """
                SELECT [CustomerID] as [cust_id],
                RTRIM(LTRIM(LOWER(ISNULL([FirstName],'')))) + ' ' +
                RTRIM(LTRIM(LOWER(ISNULL([MiddleName],'')))) + ' ' +
                RTRIM(LTRIM(LOWER(ISNULL([LastName],'')))) as [full_name]
                FROM """ + self.SRC_TABLE + """ where ([CustomerListID] = 2)
                and ( (([ModifiedDate] >= [CreatedDate]) and ([ModifiedDate] <= 
                '""" + self.TODAY + """')) or ([CreatedDate] <=
                '""" + self.TODAY + """'));
                """
        
        # cust_id | full_name
        self.TODAYS_CUSTOMERS = pd.read_sql(pick_customers_qry, self._ENGINE)
        # note: can be either engine or connection, i.e. SQLAlchemy connectable
    
        print("collected {} ({} unique) customer ids from table {}".format(len(self.TODAYS_CUSTOMERS),
                    len(self.TODAYS_CUSTOMERS["cust_id"].unique()),self.SRC_TABLE))
            
        return self
    
    def upload_ethnicities(self, df_ethn):
         
        # what customer ids are already in the ethnicity table?

        try:
            self.IDS_IN_TABLE = set(pd.read_sql("SELECT [CustomerID] FROM " + self.TARGET_TABLE + ";", 
                                            self._ENGINE)["CustomerID"].astype(str))
        except:
            
            self.IDS_IN_TABLE = set()
        
        print('unique customer ids already in the ethnicity table {}: {}'.format(self.TARGET_TABLE, len(self.IDS_IN_TABLE)))

        # check: any customer ids that are among the new ones but somehow also sit in the ethnicity table?

        self.IDS_TO_UPDATE = self.IDS_IN_TABLE & set(df_ethn.CustomerID)
        print("-- customer ids to update: {}".format(len(self.IDS_TO_UPDATE)))
        # new customer ids to be appended to the ethnicity table
        self.IDS_TO_APPEND = set(df_ethn.CustomerID) - self.IDS_TO_UPDATE
        print("-- customer ids to append: {}".format(len(self.IDS_TO_APPEND)))
        # note: 'append': if table exists, insert data. Create if does not exist
        if self.IDS_TO_APPEND:
            print("appending new ethnicities to table {}...".format(self.TARGET_TABLE), end='')
            self._push_to_sql(df_ethn.loc[df_ethn.CustomerID.isin(self.IDS_TO_APPEND),['CustomerID',
                'Ethnicity']], self.TARGET_TABLE, self._ENGINE)

            print("ok")

        if self.IDS_TO_UPDATE:
            
            upd_dict = defaultdict(lambda: defaultdict(str))

            # create a temporary table with the customer ids to update
            pd.DataFrame({"CustomerID": [cid for cid in
                self.IDS_TO_UPDATE]}).to_sql('#TempIdTable', self._ENGINE,
                    if_exists="replace", dtype={"CustomerID":
                        sa.types.String(length=20)}, schema='TEGA.dbo')

            print("reading the part of {} to update...".format(self.TARGET_TABLE))
            df_to_update = pd.read_sql("SELECT * FROM " + self.TARGET_TABLE +
                    " where CustomerID in (SELECT CustomerID FROM #TempIdTable);", self._ENGINE)
            
            df_to_update["CustomerID"] = df_to_update["CustomerID"].astype(str)

            # print(df_to_update.head())

            print("setting up the updates...")

            for row in df_to_update.iterrows():
                new_cust_id = row[1].CustomerID
                # ethnicities already assigned to this customer
                new_cust_ethnicities = set(row[1].Ethnicity.split("|"))
                updated_ethnicities = "|".join(new_cust_ethnicities | set(df_ethn.loc[df_ethn.CustomerID == new_cust_id, "Ethnicity"].values[0].split("|")))
                upd_dict[new_cust_id] = {"Ethnicity": updated_ethnicities}
            
            print("creating an update data frame...")
            upd_df = pd.DataFrame.from_dict(upd_dict, orient='index').reset_index().rename(columns={"index": "CustomerID"}) 
            upd_df["CustomerID"] = upd_df["CustomerID"].astype(str)

            print(upd_df.head())

            print("deleting rows from {} for customer ids to be updated}...".format(self.TARGET_TABLE), end='')
            res = self._ENGINE.execute("DELETE FROM " + self.TARGET_TABLE +" where CustomerID in (SELECT CustomerID FROM #TempIdTable);")
            print('ok')

            print("pushing the update data frame into {}...".format(self.TARGET_TABLE), end='')
            self._push_to_sql(upd_df, self.TARGET_TABLE, self._ENGINE)
            print('ok')
    
    def send_email(self, df_ethn):
        
        sender_email, sender_pwd, smtp_server, smpt_port = [line.split("=")[-1].strip() 
                                    for line in open("config/email.cnf", "r").readlines() if line.strip()]
        
        msg = MIMEMultipart()   
        
        msg['From'] = sender_email
        msg['To'] = ','.join([sender_email])
        msg['Subject'] = 'update on ethnicities: customers modified on {}'.format(self.TODAY)
        
        dsample = pd.DataFrame()

        for k, v in Counter(df_ethn['Ethnicity']).items():
            this_ethnicity = df_ethn[df_ethn.Ethnicity == k]
            ns = 3 if len(this_ethnicity) > 2 else 1
            dsample = pd.concat([dsample, this_ethnicity.sample(n=ns)])

        st_summary  = "-- new ethnic customer ids captured today:\n\n" + \
                "".join(["{}: {}\n".format(ks.upper(), vs) for ks, vs in sorted([(k,v) 
                    for k, v in Counter(df_ethn['Ethnicity']).items()], key=lambda x: x[1], reverse=True)])
        
        msg.attach(MIMEText(st_summary+ "\n-- sample:\n\n" + dsample.loc[:,["CustomerID", "FullName", "Ethnicity"]].to_string(index=False, justify="left",
            formatters=[lambda _: "{:<12}".format(str(_).strip()), lambda _: "{:<30}".format(str(_).strip()), lambda _: "{:<20}".format(str(_).strip())]), 'plain'))
        server = smtplib.SMTP(smtp_server, smpt_port)
        server.starttls()
        print('sending email notification...',end='')
        server.login(sender_email, sender_pwd)
        server.sendmail(sender_email, sender_email, msg.as_string())
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
        self.surname_ending_dict = json.load(open(self.NAME_DATA_DIR + "surname_endings_06102017.json", "r"))
        self.deciders = {"arabic": "name_and_surname", "italian": "name_and_surname", 
                         "filipino": "name_and_surname", "indian": "name_or_surname",
                         "japanese": "name_and_surname"}
        # self.ethn_abbrs = {"arabic": "ar", "italian": "it", "filipino": "ph", "indian": "in", "japanese": "jp"}
        
        # make name and surname dictionaries by letter for required ethnicities
        self.names = defaultdict(lambda: defaultdict(set))
        self.surnames = defaultdict(lambda: defaultdict(set))
    
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
                                                                      [1 for name_prt in _.split() if name_prt in self.names[ethnicity][name_prt[0]]]]))
        
        self.input_df.loc[:, "s_ethn"] = self.input_df["full_name"].apply(self.__search_surname)
             
        return self 
    
    def pick_ethnicity(self):
        
        x_ethns = self.input_df[(self.input_df["n_ethn"].str.len() > 1) | (self.input_df["s_ethn"].str.len() > 1)]
        
        final_ethnicities = defaultdict(lambda: defaultdict(str))
        
        for row in x_ethns.iterrows():
            
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
        print("total new customer ids we detected ethnicity for: {}".format(len(self._detected_ethnicities.CustomerID.unique())))

        return self

if __name__ == '__main__':
    
    tc = TableHandler(driver='pymssql')

    def job():
        
        print('job started at {}...'.format(datetime.now()))

        t0 = time.time()
        
        tc.connect('src')
        tc.get_customers()
        tc.disconnect()
        
        if len(tc.TODAYS_CUSTOMERS) == 0:

            print('found no new customers on {}..'.format(tc.TODAY))
            #tc.send_email(tc.TODAYS_CUSTOMERS)
        else:
             ed = EthnicityDetector(tc.TODAYS_CUSTOMERS, ["indian", "filipino", "japanese", "arabic", "italian"]).clean_input()
             ed.find_ethnicity_candidates().pick_ethnicity()
             
             #tc.send_email(ed._detected_ethnicities)

             tc.connect('src')
             tc.upload_ethnicities(ed._detected_ethnicities)
             tc.disconnect()
        
        tc.send_email(ed._detected_ethnicities)

        print("elapsed time: {:.0f} minutes {:.0f} seconds".format(*divmod(time.time() - t0, 60)))

    schedule.every().day.at('04:55').do(job)
    
    while True:
        schedule.run_pending()
