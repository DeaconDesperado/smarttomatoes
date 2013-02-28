import sys
import json
import re
from time import sleep
from sets import Set
from config import Config
import minimongo

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

    print mapped



def main():
    if not checkCache():
        buildCache()
        makeCritics()
    most_prolific()

main()
