import itertools as it
from collections import defaultdict, Counter

class NameFeatureExtractor(object):

	def __init__(self):

		# features will be a set of tuples (feature name, feature value)
		self.features = set()

	def _normalise(self, st):
		"""
		normalise the full name so that it look like 'john smith', i.e. 
			* words separated by a single white space
			* all lower case
			* only letters allowed
		"""
		pass 

	def _get_number_words(self, st):
		"""
		count the number of words separated by white space
		"""
		self.features.add(('n_words', len(st.split())))

	def _get_first_letters(self, st):
		"""
		count the first letters of each word in full name; 
		example: 'john j smith' shoud result in ('fst_let_j', 2), ('fst_let_s', 1)
		"""
		for l, cnt in Counter([w[0] for w in st.split()]).items():
			self.features.add(('fst_let_' + l, cnt))

	def _get_letter_counts(self, st):
		"""
		count ocurrences of all letters in fuill name
		"""
		for l, cnt in Counter(st.replace(' ', '')).items():
			self.features.add(('cnt_let_' + l, cnt))

	def get_features(self, st):
		"""
		extract all features for the given full name into a dictionary
		"""
		self._get_number_words(st)
		self._get_first_letters(st)
		self._get_letter_counts(st)

		return self

if __name__ == '__main__':
	 ne = NameFeatureExtractor()
	 print(ne.get_features('britney bob spears').features)
