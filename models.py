import minimongo
from minimongo import Model,Index
import re
from config import Config
from rottentomatoes import RT
from time import sleep
from pymongo.errors import DuplicateKeyError

rtapi = RT(Config.API_KEY)

def mongo_config(config,prefix='MONGODB_'):
    attrs = config.__dict__.iteritems()
    attrs = ((attr.replace(prefix, '').lower(), value)
             for attr, value in attrs if attr.startswith(prefix))
    minimongo.configure(**dict(attrs))
mongo_config(Config)

class Critic(Model):
    class Meta:
        collection = 'critics'
        indices = [
            Index('ratings.movie_id',1)        
        ]

    @classmethod
    def create(cls,data):
        return Critic(data)

class Movie(Model):
    class Meta:
        collection = 'movies'

    @classmethod
    def create(cls,movie):
        movie['formatted_title'] = re.sub('[\s|:]+','_',movie['title'].lower())
        movie['_id'] = movie['id']
        print 'Saving %s id %s' % (movie['title'], movie['_id'])
        return Movie(movie)


class Review(Model):
    class Meta:
        collection = 'reviews'
        indices = [
            Index([('critic',1),('movie_id',1)],unique=True)
        ]

    @classmethod
    def create(cls,review,movie_id):
        review['movie_id'] = movie_id
        review = Review(review)
        review.convert_rating()
        return review

    def convert_rating(self):
        return float(float(self['original_score'].split('/')[0])/4)


def checkCache():
    movies = Movie.collection.find()
    reviews = Review.collection.find()
    if movies.count() < 20 and reviews.count() < 20:
        print 'build cache'
        return False
    return True

def buildCache():
    page_no = 0
    for page in xrange(Config.CACHE_SIZE/10):
        page_no+=1
        movies = rtapi.new('dvds',page=page_no,page_limit=10)
        for i in xrange(len(movies)):
            movie = Movie.create(movies[i])
            movie.save()
            reviews = rtapi.info(movie._id,'reviews')
            for review in reviews['reviews']:
                try:
                    review = Review.create(review,movie._id)
                except (KeyError,ValueError,IndexError):
                    continue
                print 'Saving review from %s' % review.get('critic','Nobody')
                try:
                    review.save(safe=True)
                    sleep(1)
                except DuplicateKeyError:
                    continue
            sleep(1)
    
    for review in Review.collection.find():
        critic = Critic.find_one({'_id':review['critic']})
        if not critic:
            critic = Critic.create(dict(_id=review['critic']))
        reviews = critic.get('ratings',{})
        if review['movie_id'] not in reviews.keys():
            reviews[review['movie_id']]=rating
        critic['ratings'] = reviews
        critic['prevalence'] = len(reviews)
        critic.save(critic)
        print '%s rated id %s %s' % (critic['_id'],review['movie_id'],rating)



