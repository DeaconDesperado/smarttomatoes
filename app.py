import sys
import json
import re
from time import sleep
from sets import Set
from config import Config
from models import *

def most_prolific():
    critics = Critic.find().sort('prevalence',-1).limit(20)
    common = Set()
    crits = Set()
    for critic in critics:
        if len(common) == 0:
            common = Set(critic['ratings'].keys())
        elif len(common) < 4:
            break
        else:
            common = common.intersection(Set(critic['ratings'].keys()))
        crits.add(critic['_id'])
    print common,crits

def main():
    if not checkCache():
        buildCache()
    most_prolific()

main()
