import re
import json
from unidecode import unidecode
from collections import defaultdict
from string import ascii_lowercase   # 'abcdefghijklmnopqrstuvwxyz'

class EthnicityDetector(object):

    """
    detect ethnicity given a string containing a full name
    """
    
    def __init__(self, eth_lst):
        
        self.DATA_DIR = "/Users/ik/Data/" 
        self.NAME_DATA_DIR = self.DATA_DIR + "names/"
        self.ETHNICITY_LIST = eth_lst
        print(self.ETHNICITY_LIST)
        
        # load name and surname databases
        self.name_dict = json.load(open(self.NAME_DATA_DIR + "names_26092017.json", "r"))
        self.surname_dict = json.load(open(self.NAME_DATA_DIR + "surnames_26092017.json", "r"))
        self.names_international = {line.strip().lower() for line in open(self.NAME_DATA_DIR + "names_international.txt", "r").readlines() if line}
        self.surname_ending_dict = json.load(open(self.NAME_DATA_DIR + "surname_endings_06102017.json", "r"))
        
        # note: name AND surname exactly matched is the obvious choice
        self.deciders = {"name_or_surname": {"indian", "japanese", "greek"},
                            "name_only": {"thai", "arabic"},
                                "surname_only": {"vitnamese", "serbian", "italian"}}
        
        # make name and surname dictionaries by letter for required ethnicities
        self.names = defaultdict(lambda: defaultdict(set))
        self.surnames = defaultdict(lambda: defaultdict(set))
    
    def _create_ethnic_dicts(self):
        
        for ethnicity in self.ETHNICITY_LIST:
            
            if ethnicity in self.name_dict:
                self.names[ethnicity] = {letter: {unidecode(w["name"]) for w in self.name_dict[ethnicity] 
                                                 if w["name"].isalpha() and unidecode(w["name"][0]) == letter} for letter in ascii_lowercase}
                
            if ethnicity in self.surname_dict:
                self.surnames[ethnicity] = {letter: {unidecode(w) for w in self.surname_dict[ethnicity] 
                                                 if w.isalpha() and unidecode(w)[0] == letter} for letter in ascii_lowercase}

        return self
    
    def _normalise_string(self, st):

        # lower case
        # get ASCII transliteration of Unicode (to avoid, for example, Spanish letters)
        st = unidecode(st.lower())
        # replace separators with white spaces
        st = re.sub(r'[-,_/().]',' ', st)
        # remove all non-letters
        st = ' '.join(''.join([c for c in st if c in ascii_lowercase + ' ']).split())

        return st
    
    def get_ethnicity(self, st):
        
        self._create_ethnic_dicts()

        st = self._normalise_string(st)

        mtchd = {"name": set(), "surname": set()}
        
        for name_prt in st.split():

            if len(name_prt) < 2:
                continue
            else:
                first_l = name_prt[0]   # first letter 
                print("1st letter: ",first_l)              
                # try to match exact last name
                print("matching exact surname..")
                for ethnicity in self.ETHNICITY_LIST:
                    try:
                        if name_prt in self.surnames[ethnicity][first_l]:
                            mtchd["surname"].add(ethnicity)
                    except:
                        pass
                # if exact match didn't work, try last name endings
                if (not mtchd["surname"]):
                    print('couldnt match exactly, looking at endings..')
                    for ethnicity in self.ETHNICITY_LIST:
                        if ethnicity in self.surname_ending_dict:
                            for ending in self.surname_ending_dict[ethnicity]:
                                if name_prt.endswith(ending) and (len(name_prt) - len(ending) > 1):
                                    mtchd["surname"].add(ethnicity)
                print("after surname search:", mtchd)
                # search for name
                if name_prt in self.names_international:
                    print('international name!')
                    continue
                else:
                    print("name not international..")
                    for ethnicity in self.ETHNICITY_LIST:
                        try:
                            print("trying to match name..")
                            if name_prt in self.names[ethnicity][first_l]:
                                mtchd["name"].add(ethnicity)
                                print("matched name ", ethnicity)
                        except:
                            print("didnt find in names")
                        pass
       
        # maybe there's an ethnicity we found both name and surname for
        oked  = mtchd["surname"] & mtchd["name"]
        print("both name and surmame?", oked)

        if not oked:

            for ethnicity in mtchd["surname"]:
                print("chekcing deciders for ", ethnicity)
                if ethnicity in self.deciders["surname_only"] | self.deciders["name_or_surname"]:
                    oked.add(ethnicity)
            for ethnicity in mtchd["name"]:
                if ethnicity in self.deciders["name_only"] | self.deciders["name_or_surname"]:
                    oked.add(ethnicity)
        print("finally, oked=", oked)
        return None if not oked else "|".join(sorted(oked))

if __name__ == '__main__':

    ed = EthnicityDetector(["japanese", "thai", "arabic", "serbian", "indian", "greek", "italian"])
    print(ed.get_ethnicity('panos callini'))
