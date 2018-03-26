import re
import json
import time
from unidecode import unidecode
from collections import defaultdict
from string import ascii_lowercase

import os

class EthnicityDetector(object):

	"""
	detect ethnicity given a string containing a full name;
	update 26/03/2018: all data is now expected to be in 'data' directory
	"""
	
	def __init__(self):

		self.NAME_DATA_DIR = os.path.join(os.path.curdir, 'data')

		self.ETHNICITY_LIST = """indian japanese greek arabic turkish
									thai vietnamese balkan italian  samoan
										hawaiian khmer chinese korean polish""".split()

		print(f'total ethnicities: {len(self.ETHNICITY_LIST)}')

		self.SEPARATORS = re.compile(r'[-,_/().]')
		
		# load name and surname databases
		self.name_dict = json.load(open(os.path.join(self.NAME_DATA_DIR, 'names.json'), "r"))
		self.surname_dict = json.load(open(os.path.join(self.NAME_DATA_DIR, 'surnames.json'), "r")) 
		self.names_international = {line.strip().lower() 
						for line in open(os.path.join(self.NAME_DATA_DIR, 'names_international.txt'),'r').readlines() if line}
		self.surname_ending_dict = json.load(open(os.path.join(self.NAME_DATA_DIR, 'surname_endings.json'), "r"))
		
		# note: name AND surname exactly matched is the obvious choice
		self.deciders = {"name_or_surname": {"indian", "japanese", "chinese"},
							"name_only": {"thai", "arabic", "turkish", "hawaiian", "samoan", "khmer", "polish"},
								"surname_only": {"vietnamese", "balkan", "italian", "korean", "greek"}}
		
		assert set(self.ETHNICITY_LIST) == {e for dec in self.deciders for e in self.deciders[dec]}, "ERROR! missing decider(s)!"

		# make name and surname dictionaries by letter for required ethnicities
		self.names = dict()
		self.surnames = dict()

		for ethnicity in self.ETHNICITY_LIST:
			
			if ethnicity in self.name_dict:
				self.names.update({ethnicity: {letter: {unidecode(w["name"]) for w in self.name_dict[ethnicity] 
												 if w["name"].isalpha() and unidecode(w["name"][0]) == letter} for letter in ascii_lowercase}})

			if ethnicity in self.surname_dict:
				self.surnames.update({ethnicity: {letter: {unidecode(w) for w in self.surname_dict[ethnicity] 
												 if w.isalpha() and unidecode(w)[0] == letter} for letter in ascii_lowercase}})		
	
	def _normalise_string(self, st):
		
		if not isinstance(st, str):
			return None

		if len(st.strip()) < 3:
			return None

		# get ASCII transliteration of Unicode (to avoid, for example, Spanish letters)
		st = unidecode(st.lower())

		# replace separators with white spaces
		st = re.sub(self.SEPARATORS,' ', st)

		# remove all non-letters
		_ = ''.join([c for c in st if c in ascii_lowercase + ' ']).strip()

		if not _:
			return None

		st = ' '.join(_.split())
	 
		return st
	
	def get_ethnicity(self, st):

		st = self._normalise_string(st)
		
		if not st:
			return None

		mtchd = {"name": set(), "surname": set()}
		
		for j, word in enumerate(st.split()):

			if len(word) < 2:
				continue
			else:
				first_l = word[0]  
				for ethnicity in self.ETHNICITY_LIST:
					try:
						if word in self.surnames[ethnicity][first_l]:
							mtchd["surname"].add(ethnicity)
					except:
						pass
				# if exact match didn't work, try last name endings
				if not mtchd["surname"]:
					for ethnicity in self.ETHNICITY_LIST:
						if ethnicity in self.surname_ending_dict:
							for ending in self.surname_ending_dict[ethnicity]:
								if word.endswith(ending) and (len(word) - len(ending) > 1):
									if ethnicity in {'italian', 'balkan', 'greek'}:
										if j > 0:
											mtchd["surname"].add(ethnicity)
									else:
										mtchd["surname"].add(ethnicity)
				# search for name
				if word in self.names_international:
					continue
				else:
					for ethnicity in self.ETHNICITY_LIST:
						try:
							if word in self.names[ethnicity][first_l]:
								mtchd["name"].add(ethnicity)
						except:
							pass
	   
		# maybe there's an ethnicity we found both name and surname for

		oked  = mtchd["surname"] & mtchd["name"]

		if not oked:

			for ethnicity in mtchd["surname"]:
				if ethnicity in self.deciders["surname_only"] | self.deciders["name_or_surname"]:
					oked.add(ethnicity)
			for ethnicity in mtchd["name"]:
				if ethnicity in self.deciders["name_only"] | self.deciders["name_or_surname"]:
					oked.add(ethnicity)
		
		if not oked:
			return None 

		asian = {'chinese', 'korean', 'japanese'}

		if asian & oked:

			name_parts = st.split()

			if len(name_parts) > 2:
				if (name_parts[-2] in ['da', 'de', 'del', 'della', 'dos', 'van']) and (len(name_parts[-1]) > 3):
					oked -= asian
			elif len(name_parts) == 2:
				if (name_parts[0] in {'joe', 'lee', 'bo', 'su', 'lou', 'kim', 'jo', 'li', 'ken', 'juan'}) and (len(name_parts[1]) > 3):
					oked -= asian
				if (len(name_parts[0]) > 4) and (name_parts[1] == 'long'):
					oked -= asian

			if 'nguyen' in name_parts:
				oked -= asian


		res = None if not oked else "|".join(sorted(oked))

		# if too many possible ethnicities discard all
		if res and (res.count("|") > 2):
			res = None
			
		return res 

if __name__ == '__main__':

	ed = EthnicityDetector()
	print(ed.get_ethnicity('tomasz mozolli'))
