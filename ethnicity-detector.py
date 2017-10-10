import pandas as pd
import json
import sys
from datetime import datetime, timedelta, time
from collections import defaultdict, Counter
from unidecode import unidecode
from string import ascii_lowercase
import sqlalchemy as sa
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import schedule
import pytz


class TableCollector(object):
    
    def __init__(self, driver='pyodbc', conf_file = 'connection-loc.ini', 
            src_table='[DWSales].[dbo].[tbl_LotusCustomer]', target_table='CustomerEthnicities'):
        
        self.DRIVER_NAME = driver
        self.SRC_TABLE = src_table
        self.TARGET_TABLE = target_table
        self.CONF_FILE = conf_file
        
        assert self.DRIVER_NAME in ['pyodbc', 'pymssql'], "wrong driver name - choose pyodbc or pymssql"
        
        try:
            self.SERVER, self.USER, self.PORT, self.PWD, self.DRIVER, self.DB_NAME = [line.split("=")[-1].strip() 
                    for line in open("config/" + self.CONF_FILE, 'r').readlines() if line.strip()] 
            self.USER = r'TT\igork'
        except:
            print("problem with configuration file, exiting..")
            sys.exit(0)
            
    def connect(self):
        
        self.CONNSTR = 'mssql+{}://{}:{}@{}:{}/{}'.format(self.DRIVER_NAME, self.USER, self.PWD, 
                                                          self.SERVER, self.PORT, self.DB_NAME)
        
        if self.DRIVER_NAME == 'pyodbc':
            import pyodbc
            self.CONNSTR = 'mssql+{}://{}/{}'.format(self.DRIVER_NAME, self.SERVER, self.DB_NAME)

            self.CONNSTR += '?driver=' + self.DRIVER + '?trusted_connection=yes'
        elif self.DRIVER_NAME == 'pymssql':
            import pymssql
            
        try:
            self._ENGINE = sa.create_engine(self.CONNSTR)
        except:
            print('can\'t create engine, exiting..')
            sys.exit(0)

        self._CONNECTION = self._ENGINE.connect()
        
        return self
    
    
    def disconnect(self):

        self._CONNECTION.close()
    

    def get_customers(self):
        
        self.TODAY = (datetime.now() + timedelta(days=-1)).strftime("%Y%m%d")
        #self.TODAY = '20170301'
        print(self.TODAY)

        pick_customers_qry = """
                SELECT [CustomerID] as [cust_id],
                RTRIM(LTRIM(LOWER(ISNULL([FirstName],'')))) + ' ' +
                RTRIM(LTRIM(LOWER(ISNULL([MiddleName],'')))) + ' ' +
                RTRIM(LTRIM(LOWER(ISNULL([LastName],'')))) as [full_name]
                FROM """ + self.SRC_TABLE + """ where ([CustomerListID] = 2) and ([ModifiedDate] between '20170201' and '""" + self.TODAY + "')"
        
        self.TODAYS_CUSTOMERS = pd.read_sql(pick_customers_qry, self._ENGINE)
        # note: can be either engine or connection, i.e. SQLAlchemy connectable
            
        return self
    
    def upload_ethnicities(self, df_ethn):
         
        # check if the target table exists

        try:
            self.IDS_IN_TABLE = set(pd.read_sql("SELECT [CustomerID] FROM " + self.TARGET_TABLE + ";", 
                                            self._ENGINE)["CustomerID"])
        except:
            self.IDS_IN_TABLE = set()

        print("-- customer ids already in ethnicity table: {}".format(len(self.IDS_IN_TABLE)))

        # check: any customer ids that are among the new ones but somehow also sit in the ethnicity table?
        self.IDS_TO_UPDATE = self.IDS_IN_TABLE & set(df_ethn.CustomerID)
        print("-- customer ids to update: {}".format(len(self.IDS_TO_UPDATE)))
        # new customer ids to be appended to the ethnicity table
        self.IDS_TO_APPEND = set(df_ethn.CustomerID) - self.IDS_TO_UPDATE
        print("-- customer ids to append: {}".format(len(self.IDS_TO_APPEND)))
        # note: 'append': if table exists, insert data. Create if does not exist
        print("uploading new ethnic customer ids...", end='')
        df_ethn.loc[df_ethn.CustomerID.isin(self.IDS_TO_APPEND),
                ['CustomerID', 'Ethnicity']].to_sql(self.TARGET_TABLE, self._ENGINE, if_exists='append', 
                        index=False, dtype={"CustomerID": sa.types.String(length=20),
                                                "Ethnicity": sa.types.String(length=20)})

        print("ok")

        if self.IDS_TO_UPDATE:
            
            upd_dict = defaultdict(lambda: defaultdict(str))

            df_to_update = pd.read_sql("SELECT * FROM " + self.TARGET_TABLE + "where CustomerID in " + "(" + ",".join(self.IDS_TO_UPDATE) + ");", 
                                            self._ENGINE)
            
            for row in df_to_update.iterrows():
                new_cust_id = row[1].CustomerID
                # ethnicities already assigned to this customer
                new_cust_ethnicities = set(row[1].Ethnicity.split("|"))
                updated_ethnicities = "|".join(new_cust_ethnicities | set(df_ethn.loc[df_ethn.CustomerID == new_cust_id, "Ethnicity"].values[0].split("|")))
                upd_dict[new_cust_id] = {"Ethnicity": updated_ethnicities}
            
            upd_df = pd.DataFrame.from_dict(upd_dict, orient='index').reset_index().rename(columns={"index": "CustomerID"}) 
            upd_df.loc[upd_df, :].to_sql(self.TARGET_TABLE, self._ENGINE, if_exists='append', index=False, 
                    dtype={"CustomerID": sa.types.String(length=20), "Ethnicity": sa.types.String(length=20)})
    
    def send_email(self, df_ethn):
        
        sender_email, sender_pwd, smtp_server, smpt_port = [line.split("=")[-1].strip() 
                                    for line in open("config/email.cnf", "r").readlines() if line.strip()]
        
        msg = MIMEMultipart()   
        
        msg['From'] = sender_email
        msg['To'] = sender_email
        msg['Subject'] = 'updates on ethnicities - {}'.format(self.TODAY)
        
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
                         "filipino": "name_and_surname", "indian": "surname",
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
                                                                .str.replace("'", " ").str.replace("-", " ").str.replace("(", " ").str.replace(")", " ")
                                                                  .str.split().str.join(' '))

        # only keep names that have parts consisting of letters and longer than a single character; places '' where no proper full names
        self.input_df.loc[:, "full_name"] = self.input_df["full_name"].apply(lambda _: " ".join([p for p in _.split() 
                                                                                                 if p.isalpha() and len(p) > 1])).str.strip()
        self.input_df = self.input_df.loc[self.input_df["full_name"].apply(lambda x: isinstance(x, str) and len(str(x).strip()) > 0), :]
        
        return self
    
    def __search_surname(self, st):
        
        exmtch_s_ethns = []
        
        for ethnicity in self.ethnicity_list:
            if ethnicity in self.surnames:
                for name_prt in st.split():
                    if name_prt in self.surnames[ethnicity][name_prt[0]]:
                        exmtch_s_ethns.append(ethnicity) 
        
        if (not exmtch_s_ethns):
            for ethnicity in self.ethnicity_list:
                if ethnicity in self.surname_ending_dict:
                    for ending in self.surname_ending_dict[ethnicity]:
                        for name_prt in st.split():
                            if name_prt.endswith(ending) and (len(name_prt) - len(ending) > 1):
                                exmtch_s_ethns.append(ethnicity)
        
        return "|".join(exmtch_s_ethns)
            
    
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
            customer_ethnicities = []
            for ethnicity in self.ethnicity_list:
                if self.deciders[ethnicity] == "name":
                    if ethnicity in row[1].n_ethn:
                        customer_ethnicities.append(ethnicity)
                elif self.deciders[ethnicity] == "surname":
                    if ethnicity in row[1].s_ethn:
                        customer_ethnicities.append(ethnicity)
                elif self.deciders[ethnicity] == "name_and_surname":
                    if (ethnicity in row[1].s_ethn) and (ethnicity in row[1].n_ethn):
                        customer_ethnicities.append(ethnicity)
            # customer_ethnicities = [self.ethn_abbrs[e] for e in customer_ethnicities]
            final_ethnicities[row[1].cust_id] = {"FullName": row[1].full_name, "Ethnicity" : "|".join(customer_ethnicities) 
            										if customer_ethnicities else None}
        
        self._detected_ethnicities = pd.DataFrame.from_dict(final_ethnicities, orient='index').reset_index().rename(columns={"index": "CustomerID"}).dropna()
       
        return self

if __name__ == '__main__':
    
    tc  = TableCollector(driver='pymssql')

    def job():
        
        tc.connect()
        tc.get_customers()
        tc.disconnect()
        
        if len(tc.TODAYS_CUSTOMERS) == 0:
            print('Found NO new customers on {}..'.format(tc.TODAY))
            tc.send_email(tc.TODAYS_CUSTOMERS)
        else:
             ed = EthnicityDetector(tc.TODAYS_CUSTOMERS, ["indian", "filipino",
                 "japanese", "arabic", "italian"]).clean_input()

             ed.find_ethnicity_candidates().pick_ethnicity()
             tc.send_email(ed._detected_ethnicities)
             tc.upload_ethnicities(ed._detected_ethnicities)

    schedule.every().tuesday.at('07:10').do(job)
    
    while True:
        schedule.run_pending()
