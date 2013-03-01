from smarttomatoes import listMatches,getRecsWeighted,setup,most_prolific
from flask import Flask,request,g
from werkzeug.wrappers import Request,Response
import json
from tornado.wsgi import WSGIContainer
from tornado.ioloop import IOLoop,PeriodicCallback
from tornado.web import FallbackHandler, RequestHandler, Application
from threading import Thread,Event
from Queue import Queue,Empty
from time import sleep
app = Flask(__name__)

setup()
mapped = most_prolific()
next_index = Queue()

#TODO: These tasks should be redis pubsub
def indexTask(queue,cancel):
    while not cancel.isSet():
        sleep(60)
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
    return Response('foo')

@app.route('/<critic>')
def critic(critic):
    resp_string = json.dumps(mapped[critic],indent=4)
    return Response(resp_string,mimetype='application/json')

@app.route('/<critic>/similarity')
def critic_similarity_matrix(critic):
    resp_string = json.dumps(dict([(key,value) for value,key in listMatches(mapped,critic)]),indent=4)
    return Response(resp_string,mimetype='application/json')

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
    index_watcher = Thread(target=indexTask,args=(next_index,cancel))
    index_watcher.start()
    try:
        PeriodicCallback(rebuild,1000,io).start()
        io.start()
    except KeyboardInterrupt,Exception:
        print 'setting cancel event'
        cancel.set()
        #shutdown
