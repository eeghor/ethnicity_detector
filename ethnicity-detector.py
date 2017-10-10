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
    
    def __init__(self, driver='pyodbc', target_table='CustomerEthnicities'):
        
        self.DRIVER_NAME = driver
        self.TARGET_TABLE = target_table
        
        assert self.DRIVER_NAME in ['pyodbc', 'pymssql'], "wrong driver name - choose pyodbc or pymssql"
        
        try:
            self.SERVER, self.USER, self.PORT, self.PWD, self.DRIVER, self.DB_NAME = [line.split("=")[-1].strip() 
            for line in open("config/connection.ini", 'r').readlines() if line.strip()]          
        except:
            print("problem with configuration file, exiting..")
            sys.exit(0)
            
    def connect(self):
        
        self.CONNSTR = 'mssql+{}://{}:{}@{}:{}/{}'.format(self.DRIVER_NAME, self.USER, self.PWD, 
                                                          self.SERVER, self.PORT, self.DB_NAME)
        
        if self.DRIVER_NAME == 'pyodbc':
            import pyodbc
            self.CONNSTR += '?driver=' + self.DRIVER
        elif self.DRIVER_NAME == 'pymssql':
            import pymssql
            
        self._ENGINE = sa.create_engine(self.CONNSTR)
        self._CONNECTION = self._ENGINE.connect()
        
        return self
    
    
    def disconnect(self):

        self._CONNECTION.close()
    

    def get_customers(self):
        
        self.TODAY = (datetime.now() + timedelta(days=-1)).strftime("%Y%m%d")
        self.TODAY = '20170301'
        print(self.TODAY)

        pick_customers_qry = """
                SELECT [CustomerID] as [cust_id],
                RTRIM(LTRIM(LOWER(ISNULL([FirstName],'')))) + ' ' +
                RTRIM(LTRIM(LOWER(ISNULL([MiddleName],'')))) + ' ' +
                RTRIM(LTRIM(LOWER(ISNULL([LastName],'')))) as [full_name]
                FROM [DWSales].[dbo].[tbl_LotusCustomer]
                where ([CustomerListID] = 2) and ([ModifiedDate] = '""" + self.TODAY + "')"
        
        self.TODAYS_CUSTOMERS = pd.read_sql(pick_customers_qry, self._ENGINE)
        # note: can be either engine or connection, i.e. SQLAlchemy connectable
            
        return self
    
    def upload_ethnicities(self, df_ethn):
         
        self.IDS_IN_TABLE = set(pd.read_sql("SELECT [CustomerID] FROM " + self.TARGET_TABLE + ";", 
                                            self._ENGINE)["CustomerID"])
        
        print("ids already in table: {}".format(len(self.IDS_IN_TABLE)))
        # check: any customer ids that are among the new ones but somehow also sit in the ethnicity table?
        self.IDS_TO_UPDATE = self.IDS_IN_TABLE & set(df_ethn.CustomerID)
        print("ids to update: {}".format(len(self.IDS_TO_UPDATE)))
        # new customer ids to be appended to the ethnicity table
        self.IDS_TO_APPEND = set(df_ethn.CustomerID) - self.IDS_TO_UPDATE
        print("ids to append: {}".format(self.IDS_TO_APPEND))
        # note: 'append': if table exists, insert data. Create if does not exist
        toappend = df_ethn.loc[df_ethn.CustomerID.isin(self.IDS_TO_APPEND), ['CustomerID', 'Ethnicity']]
        print(toappend)

        
        toappend.to_sql(self.TARGET_TABLE, self._ENGINE, if_exists='append', index=False, dtype={"CustomerID": sa.types.String(length=20),
            "Ethnicity": sa.types.String(length=2)})

        print("upload complete..")

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
            upd_df.loc[upd_df, :].to_sql(self.TARGET_TABLE, self._ENGINE, if_exists='append', index=False, dtype={"CustomerID": sa.types.String(length=20),
            "Ethnicity": sa.types.String(length=2)})
    
    def __make_report_string(self, df_ethn):
        
        if len(df_ethn) > 0:	

        	st = ["{} Ethnicities {}".format(['-'*6]*2)]
        	
        	for k, v in Counter(df_ethn["Ethnicity"]).items():  # returns a list of tuples
        	    st.append( "{:>6}{}{:<6}".format(k, 19*' ', v))
        	    
        	return "\r\n".join(st)
        else:
        	return("No new ethnicities found today.")
    
    def send_email(self, df_ethn):
        
        sender_email, sender_pwd, smtp_server, smpt_port = [line.split("=")[-1].strip() 
                                    for line in open("config/email.cnf", "r").readlines() if line.strip()]
        
        msg = MIMEMultipart()   

        body = df_ethn.to_html()
        
        msg['From'] = sender_email
        msg['To'] = sender_email
        msg['Subject'] = 'updates on ethnicities - {}'.format(self.TODAY)
        
        msg.attach(MIMEText(self.__make_report_string(df_ethn), 'html'))
        #msg.attach(MIMEText(body, 'html'))
        
        server = smtplib.SMTP(smtp_server, smpt_port)
        server.starttls()
        print('login into server {}'.format(smtp_server))
        server.login(sender_email, sender_pwd)
        print('sending email..')
        server.sendmail(sender_email, sender_email, msg.as_string())
        print('sent')
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
    
        self.input_df["n_ethn"] = self.input_df["full_name"].apply(lambda _: "|".join([ethnicity for ethnicity in self.ethnicity_list if 1 in 
                                                                      [1 for name_prt in _.split() if name_prt in self.names[ethnicity][name_prt[0]]]]))
        
        self.input_df["s_ethn"] = self.input_df["full_name"].apply(self.__search_surname)
             
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


# In[172]:


if __name__ == '__main__':
    
    tc  = TableCollector()

    def job():
        
        tc.connect()
        tc.get_customers()

        print(tc.TODAYS_CUSTOMERS.head())

        tc.disconnect()
        
        if len(tc.TODAYS_CUSTOMERS) == 0:
            print('Found NO new customers on {}..'.format(tc.TODAY))
            tc.send_email(tc.TODAYS_CUSTOMERS)
        else:
             ed = EthnicityDetector(tc.TODAYS_CUSTOMERS, ["indian", "filipino", "japanese", "arabic", "italian"])
             print('initialized ED')
             ed.clean_input()
             print('cleaned data')
             ed.find_ethnicity_candidates().pick_ethnicity()
             print(ed._detected_ethnicities.head())
             tc.send_email(ed._detected_ethnicities)
             # tc.connect()
             tc.upload_ethnicities(ed._detected_ethnicities)
             # tc.disconnect()

    # JOB_TIME_SYDNEY = '12:00'

    # JOB_TIME_UTC = pytz.utc.localize(time(*[int(p) for p in JOB_TIME_SYDNEY.split(":")]))
    schedule.every().monday.at('05:39').do(job)
    
    while True:
        schedule.run_pending()
