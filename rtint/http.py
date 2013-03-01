from rtint import mapped,listMatches,getRecsWeighted
from flask import Flask,request
from werkzeug.wrappers import Request,Response
import json

app = Flask(__name__)

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

if __name__ == '__main__':
    app.debug = True
    app.run()
