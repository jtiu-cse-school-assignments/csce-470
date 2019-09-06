from flask import Flask, jsonify, session, url_for
from flask_cors import CORS, cross_origin
import requests
import json
import pandas as pd
import numpy as np

'''
How to run: 

export FLASK+APP=app.py
flask run
'''

app = Flask(__name__)
CORS(app, support_credentials=True)

@app.route('/')
def hello_world():
    return 'Hello, World!'

@app.route('/search/<genre>')
@cross_origin(supports_credentials=True)
def generateDirecotrsList(genre):  
    genre = genre[0].upper() + genre[1:]
    listOfDirectors = []
    listOfDirectorsRatings = {}
    listOfDirectorsRatingsSorted = []
    listToReturnToFrontEnd = {}
    
    with open('moviesOfAllDirectors.json') as f:
        data = json.load(f)
    
    with open(f'./directorsInGenre/listOfDirectorsIn{genre}.txt', 'r') as f:
        directors = f.readlines()
        directors = [x.strip() for x in directors] 

    with open('directorAverageRatings.json') as f2:
        data2 = json.load(f2)
    
    for line in directors:
        line = line.split(',')
        for director in line:
            if director != '\\N':
                listOfDirectors.append(director)
                try:
                    listOfDirectorsRatings[director] = data2[director]
                except KeyError:
                    pass
                    
    listOfDirectorsRatingsSorted = sorted(listOfDirectorsRatings.items(), key=lambda x: x[1], reverse=True)
    
    for i in range(10):
        listOfMoviesByThisDirector = []
        for j in  data[listOfDirectorsRatingsSorted[i][0]]:
            contents = requests.get(f"http://localhost:8983/solr/title-basics/select?df=tconst&fl=originalTitle&q={j}")
            nameOfMovie = contents.json()
            if nameOfMovie["response"]["numFound"] > 0:
                listOfMoviesByThisDirector.append(nameOfMovie["response"]["docs"][0]["originalTitle"][0])
        contents = requests.get(f"http://localhost:8983/solr/name-basics/select?df=nconst&fl=primaryName&q={listOfDirectorsRatingsSorted[i][0]}")
        nameOfDirector = contents.json()
        if nameOfDirector["response"]["numFound"] > 0:
            listToReturnToFrontEnd[nameOfDirector["response"]["docs"][0]["primaryName"][0]] = listOfMoviesByThisDirector
    
    return jsonify(listToReturnToFrontEnd)