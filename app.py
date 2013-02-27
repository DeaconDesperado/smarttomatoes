import sys
sys.path.append('./rottentomatoes')
from rottentomatoes import RT
import json
import os
import re
from time import sleep
import math
from pymongo import MongoClient
from sets import Set

API_KEY = 'xa8fauta39t6agk3m2x8m843'
CACHE_SIZE = 50

rtapi = RT(API_KEY)

test_db = MongoClient('localhost',27017).rtapi

def checkCache():
    movies = test_db.movies.find()
    reviews = test_db.reviews.find()
    if movies.count() < 20 and reviews.count() < 20:
        print 'build cache'
        return False
    return True

def buildCache():
    page_no = 0
    for page in xrange(CACHE_SIZE/10):
        page_no+=1
        movies = rtapi.new('dvds',page=page_no,page_limit=10)
        for i in xrange(len(movies)):
            movie = movies[i]
            movie['formatted_title'] = re.sub('[\s|:]+','_',movie['title'].lower())
            movie['_id'] = movie['id']
            print 'Saving %s id %s' % (movie['title'], movie['_id'])
            test_db.movies.save(movie)
            reviews = rtapi.info(movie['id'],'reviews')
            for review in reviews['reviews']:
                review['movie_id'] = movie['id']
                print 'Saving review from %s' % review.get('critic','Nobody')
                test_db.reviews.save(review)
                sleep(1)
            sleep(1)
    print 'Got movies, getting reviews for movies'
    sleep(3)
    
    for review in test_db.reviews.find():
        rating = convert_rating(review)
        if rating:
            critic = test_db.critics.find_one({'_id':review['critic']})
            if not critic:
                critic = dict(_id=review['critic'])
            reviews = critic.get('ratings',{})
            if review['movie_id'] not in reviews.keys():
                reviews[review['movie_id']]=rating
            critic['ratings'] = reviews
            critic['prevalence'] = len(reviews)
            test_db.critics.save(critic)
            print '%s rated id %s %s' % (critic['_id'],review['movie_id'],rating)

def convert_rating(review):
    try:
        return float(float(review['original_score'].split('/')[0])/4)
    except (KeyError,ValueError,IndexError) as e:
        print e
        return False


def main():
    if not checkCache():
        buildCache()

def most_prolific():
    critics = test_db.critics.find().sort('prevalence',-1).limit(20)
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
                
main()
most_prolific()
