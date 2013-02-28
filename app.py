import sys
import json
import re
from time import sleep
from sets import Set
from config import Config
import minimongo
from math import sqrt

def mongo_config(config,prefix='MONGODB_'):
    attrs = config.__dict__.iteritems()
    attrs = ((attr.replace(prefix, '').lower(), value)
             for attr, value in attrs if attr.startswith(prefix))
    minimongo.configure(**dict(attrs))

mongo_config(Config)
from models import *

def most_prolific():
    critics = Critic.collection.find().sort('prevalence',-1).limit(20)
    recs = {}
    for critic in critics:
        recs[critic._id] = critic.get_ratings()
    return recs 

def sim_pearson(person_a,person_b):
    """
    define the pearson similarity between two people
    across mutliple dimensions
    """
    
    commonality = Set([title_id for title_id in person_a.keys()]).intersection([title_id for title_id in person_b.keys()])
    n = len(commonality)
    if n == 0:
        return 0

    person_a_sum = sum([rate for key,rate in person_a.items() if key in commonality])
    person_b_sum = sum([rate for key,rate in person_b.items() if key in commonality])

    person_a_square = sum([pow(rate,2) for key,rate in person_a.items() if key in commonality])
    person_b_square = sum([pow(rate,2) for key,rate in person_b.items() if key in commonality])
    
    pSum = sum([person_a[key]*person_b[key] for key in person_a.keys() if key in commonality])
    num = pSum-(person_a_sum*person_b_sum/n)
    density = sqrt((person_a_square-pow(person_a_sum,2)/n) * (person_b_square-pow(person_b_sum,2)/n))
    if density == 0:
        return 0
    r = num/density
    return r
    

def listMatches(pref_mapping,person_name,n=20,sim_func=sim_pearson):
    scores = [(sim_func(pref_mapping[person_name],other),other_name) for other_name,other in pref_mapping.items() if other_name!=person_name]
    scores.sort()
    scores.reverse()
    return scores

def main():
    if not checkCache():
        buildCache()
        makeCritics()
    mapped = most_prolific()
    for key in mapped:
        scores = listMatches(mapped,key)
        print key
        for answer in scores:
            print '\t%s: %s' % answer

main()
