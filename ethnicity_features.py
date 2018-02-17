from sklearn.base import BaseEstimator, TransformerMixin
from collections import Counter
import numpy as np

class Selector(BaseEstimator, TransformerMixin):
    """
    select a columns from a data frame and return as a list
    """
    def __init__(self, col_name):
        self.col_name = col_name
    
    def fit(self, x, y=None):
        return self

    def transform(self, x):
        return '_start_' + x[self.col_name] + '_end_'

class WordCount(BaseEstimator, TransformerMixin):
    """
    select a columns from a data frame and return as a list
    """
    def __init__(self):
        pass
    
    def fit(self, x, y=None):
        return self

    def transform(self, x):
        res = x.apply(lambda _: len(_.split())).values.reshape(x.shape[0],1)
        return res

class NameLength(BaseEstimator, TransformerMixin):
    """
    return the length of the full name
    """
    def __init__(self):
        pass
    
    def fit(self, x, y=None):
        return self

    def transform(self, x):
        res = x.str.len().values.reshape(x.shape[0],1)
        return res

class FirstLast(BaseEstimator, TransformerMixin):
    """
    is the first word longer than the last one
    """
    def __init__(self):
        pass
    
    def fit(self, x, y=None):
        return self

    def transform(self, x):
        res = x.apply(lambda _: np.argmax([len(p) for i, p in enumerate(_.split()) if i in [0,len(_.split())-1]])).values.reshape(x.shape[0],1)
        return res

class VowelsShare(BaseEstimator, TransformerMixin):
    """
    is the first word longer than the last one
    """
    def __init__(self):
        pass
    
    def fit(self, x, y=None):
        return self

    def vtoc(self, s):
    	_ = [c if l in 'aeoui' else -c for l, c in Counter(s.replace(' ','')).items()]
    	return sum([x for x in _ if x > 0])/sum([x for x in _ if x < 0])

    def transform(self, x):
        return x.apply(self.vtoc).values.reshape(x.shape[0],1)


class DictFirstNameFeatures(BaseEstimator, TransformerMixin):
    """
    is the first word longer than the last one
    """
    def __init__(self, nm_list):
        self.nm_list = nm_list
    
    def fit(self, x, y=None):
        return self

    def transform(self, x):
        return x.apply(lambda _: 1 if set(_.split()) & set(self.nm_list) else 0).values.reshape(x.shape[0], 1)