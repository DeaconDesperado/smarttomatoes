import sys
import re
from time import sleep
from sets import Set
from config import Config
import minimongo
from math import sqrt
import random

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
        recs[re.sub('\s+','_',critic._id.lower())] = critic.get_ratings()
    return recs 

def sim_pearson(person_a,person_b):
    """
    the pearson similarity between two people
    across mutliple common dimensions
    """
    
    #first, get the intersection between two preference matrixes, used to filer all calculation
    commonality = Set([title_id for title_id in person_a.keys()]).intersection([title_id for title_id in person_b.keys()])

    n = len(commonality)
    if n == 0:
        return 0

    #Sum the overall ratings domain
    person_a_sum = sum([rate for key,rate in person_a.items() if key in commonality])
    person_b_sum = sum([rate for key,rate in person_b.items() if key in commonality])

    #Sum the squares of the overall ratings domain
    person_a_square = sum([pow(rate,2) for key,rate in person_a.items() if key in commonality])
    person_b_square = sum([pow(rate,2) for key,rate in person_b.items() if key in commonality])
    
    #Sum the products of person_a's score for a title and person_b's score for a title
    pSum = sum([person_a[key]*person_b[key] for key in person_a.keys() if key in commonality])
    
    #Subtract the overall positivity over the size of the domain from the product sum
    num = pSum-(person_a_sum*person_b_sum/n)

    #Calculate the pt density of all plotted domains
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

def getRecsWeighted(mapping,critic_name,sim_func=sim_pearson):
    # (rating * critic similarity)/sum(critic_similarities for title) for weighted score
    totals = {}
    simSums = {}
    for other in mapping:
        if other == critic_name: continue

        sim = sim_func(mapping[critic_name],mapping[other])
        if sim == 0: continue
        for item in mapping[other]:
            if item not in mapping[critic_name]:
                totals.setdefault(item,0)
                totals[item]+=mapping[other][item]*sim
                simSums.setdefault(item,0)
                simSums[item]+=sim
    rankings = dict([(item,(total/simSums[item])) for item,total in totals.items()])
    return rankings


if not checkCache():
    buildCache()
    makeCritics()
mapped = most_prolific()


