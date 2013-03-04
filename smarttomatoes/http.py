from smarttomatoes import listMatches,getRecsWeighted,setup,most_prolific
from flask import Flask,request,g,render_template
from werkzeug.wrappers import Request,Response
import json
from tornado.wsgi import WSGIContainer
from tornado.ioloop import IOLoop,PeriodicCallback
from tornado.web import FallbackHandler, RequestHandler, Application
from threading import Thread,Event
from Queue import Queue,Empty
from time import sleep
from redis import Redis

app = Flask(__name__)

setup()
mapped = most_prolific()
next_index = Queue()

def indexTask(queue,cancel,redis_con):
    pubsub = redis_con.pubsub()
    pubsub.subscribe('smarttomatoes')
    for msg in pubsub.listen():
        if msg['data']=='C|KILL':
            cancel.set()
            break;
        setup()
        queue.put(most_prolific())

def rebuild():
    try:
        new_data = next_index.get_nowait()
        mapped = new_data
    except Empty:
        pass

@app.route('/')
def root():
    return render_template('root.html')

@app.route('/<critic>')
def critic(critic):
    resp_string = json.dumps(mapped[critic],indent=4)
    return Response(resp_string,mimetype='application/json')

@app.route('/<critic>/similarity')
def critic_similarity_matrix(critic):
    resp_string = json.dumps(dict([(key,value) for value,key in listMatches(mapped,critic)]),indent=4)
    return Response(resp_string,mimetype='application/json')

@app.route('/favicon.ico')
def favicon():
    return Response('Not found',status=404)

@app.route('/<critic>/<other_critic>')
def compare(critic,other_critic):
    matrix = dict([(key,value) for value,key in listMatches(mapped,critic)])
    return Response(json.dumps(matrix[other_critic]),mimetype='application/json')

@app.route('/<critic>/would_like')
def would_like(critic):
    recommendations = getRecsWeighted(mapped,critic)
    return Response(json.dumps(recommendations,indent=4),mimetype='application/json')

tr = WSGIContainer(app)
application = Application([
    (r'.*',FallbackHandler, dict(fallback=tr))    
])

if __name__ == '__main__':
    application.listen(5000)
    io = IOLoop.instance()
    cancel = Event()
    redis_con = Redis()
    index_watcher = Thread(target=indexTask,args=(next_index,cancel,redis_con))
    index_watcher.start()
    try:
        PeriodicCallback(rebuild,1000,io).start()
        io.start()
    except KeyboardInterrupt,Exception:
        print 'setting cancel event'
        redis_con.publish('smarttomatoes','C|KILL')
        cancel.set()
