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
    common = Set()
    crits = Set()
    for critic in critics:
        if len(common) == 0:
            common = Set([x['movie_id'] for x in critic.ratings])
        elif len(common) < 6:
            break
        else:
            common = common.intersection(Set([x['movie_id'] for x in critic.ratings]))
        crits.add(critic)

    mapped = dict()
    for crit in crits:
        recs = crit.get_ratings()
        mapped[crit._id] = {}
        for key in common:
            mapped[crit._id][key] = recs[key]

    return mapped


def sim_pearson(person_a,person_b):
    """
    define the pearson similarity between two people
    across mutliple dimensions
    """
    
    #for this iteration, only consider common elements
    n = len(person_a)

    person_a_sum = sum(person_a.values())
    person_b_sum = sum(person_b.values())

    person_a_square = sum([pow(val,2) for val in person_a.values()])
    person_b_square = sum([pow(val,2) for val in person_b.values()])
    
    pSum = sum([person_a[key]*person_b[key] for key in person_a.keys()])
    num = pSum-(person_a_sum*person_b_sum/n)
    density = sqrt((person_a_square-pow(person_a_sum,2)/n) * (person_b_square-pow(person_b_sum,2)/n))
    if density == 0:
        return 0
    r = num/density
    return r
    

def main():
    if not checkCache():
        buildCache()
        makeCritics()
    mapped = most_prolific()
    for key in mapped:
        others = [other_key for other_key in mapped.keys() if other_key != key]
        for next in others:
            print '%s->%s = %s' % (key,next,sim_pearson(mapped[key],mapped[next]))

main()
