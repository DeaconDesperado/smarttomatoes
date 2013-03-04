import minimongo
from minimongo import Model,Index
import re
from config import Config
from rottentomatoes import RT
from time import sleep
from pymongo.errors import DuplicateKeyError
import redis

rtapi = RT(Config.API_KEY)
#redis_con = redis.Redis(Config.REDIS_HOST,Config.REDIS_PORT)

class Critic(Model):
    class Meta:
        collection = 'critics'
        indices = [
            Index('ratings.movie_id',1)        
        ]

    @classmethod
    def create(cls,data):
        return Critic(data)

    def get_ratings(self):
        return dict([(r['movie_id'],r['rating']) for r in self.ratings])

    def __hash__(self):
        return hash('%s-%s' % (self._id,len(self.ratings)))

    def __eq__(self,other):
        try:
            this = '%s-%s'%(self._id,len(self.ratings))
            that = '%s-%s'%(other._id,len(other.ratings))
            return this==that
        except AttributeError:
            return False

class Movie(Model):
    class Meta:
        collection = 'movies'

    @classmethod
    def create(cls,movie):
        movie['formatted_title'] = re.sub('[\s|:]+','_',movie['title'].lower())
        movie['_id'] = movie['id']
        print 'Saving %s id %s' % (movie['title'], movie['_id'])
        return Movie(movie)

    def __hash__(self):
        return hash('%s' % (self._id))

    def __eq__(self,other):
        try:
            this = '%s' % (self._id)
            that = '%s' % (other._id)
            return this==that
        except AttributeError:
            return False


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
        review.rating = review.convert_rating()
        return review

    def convert_rating(self):
        try:
            if '/' in self['original_score']:
                numerator,denominator = self['original_score'].split('/')
                return float(numerator)/float(denominator) 
            else:
                raise ValueError()
        except ValueError:
            mod = 0
            if '+' in self['original_score']:
                mod += .0769230
            elif '-' in self['original_score']:
                mod -= .0769230
            return ('FDCBA'.index(self['original_score'].rstrip('+-')))*(3*.0769230)+mod 


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
                except KeyError:
                    continue
                print 'Saving review from %s rated %f/1' % (review.get('critic','Nobody'), review.rating)
                try:
                    review.save(safe=True)
                    sleep(1)
                except DuplicateKeyError:
                    continue
            sleep(1)

def makeCritics():
    for review in Review.collection.find():
        critic = Critic.collection.find_one({'_id':review['critic']})
        if not critic:
            critic = Critic.create(dict(_id=review['critic']))
            critic.save()
        review_slug = {'movie_id':review['movie_id'],'rating':review['rating']}
        Critic.collection.update({'_id':review['critic']},{'$push':{'ratings':review_slug}})
        critic = Critic.collection.find_one({'_id':review['critic']})
        critic.prevalence = len(critic.ratings)
        critic.save()

