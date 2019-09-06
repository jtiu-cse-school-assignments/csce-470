import pandas as pd
import numpy as np
import urllib.request
import shutil
import requests
import json
import os
from statistics import mean 

def main():
  #tsvToCsv('title.basics.tsv')
  #processTitleBasicsCsv()
  #generateAListOfGenres()

  #tsvToCsv('name.basics.tsv')
  #processNameBasicsCsv()

  #tsvToCsv('title.crew.tsv')
  #processTitleCrewCsv()

  #tsvToCsv('title.ratings.tsv')
  #processTitleRatings()

  #getAllMoviesInGenre()

  #generateAJsonOfMoviesForAllDirectors()

  #readMoviesOfAllDirectorsJson()

  #generateJsonAverageRatingForEachDirector()

  # generateDirectorsListForGenre('moviesInAdult.json', 'Adult')
  # generateDirectorsListForGenre('moviesInSci-Fi.json', 'Sci-Fi')
  # generateDirectorsListForGenre('moviesInRomance.json', 'Romance')
  # generateDirectorsListForGenre('moviesInThriller.json', 'Thriller')
  # generateDirectorsListForGenre('moviesInWestern.json', 'Western')
  # generateDirectorsListForGenre('moviesInWar.json', 'War')
  # generateDirectorsListForGenre('moviesInAnimation.json', 'Animation')
  # generateDirectorsListForGenre('moviesInHistory.json', 'History')
  # generateDirectorsListForGenre('moviesInReality-TV.json', 'Reality-TV')
  # generateDirectorsListForGenre('moviesInBiography.json', 'Biography')
  # generateDirectorsListForGenre('moviesInMusic.json', 'Music')
  # generateDirectorsListForGenre('moviesInDocumentary.json', 'Documentary')
  # generateDirectorsListForGenre('moviesInSport.json', 'Sport')
  # generateDirectorsListForGenre('moviesInMusical.json', 'Musical')
  # generateDirectorsListForGenre('moviesInTalk-Show.json', 'Talk-Show')
  # generateDirectorsListForGenre('moviesInFantasy.json', 'Fantasy')
  # generateDirectorsListForGenre('moviesInMystery.json', 'Mystery')
  # generateDirectorsListForGenre('moviesInComedy.json', 'Comedy')
  # generateDirectorsListForGenre('moviesInCrime.json', 'Crime')
  # generateDirectorsListForGenre('moviesInAdventure.json', 'Adventure')
  # generateDirectorsListForGenre('moviesInShort.json', 'Short')
  # generateDirectorsListForGenre('moviesInDrama.json', 'Drama')
  # generateDirectorsListForGenre('moviesInNews.json', 'News')
  # generateDirectorsListForGenre('moviesInFilm-Noir.json', 'Film-Noir')
  # generateDirectorsListForGenre('moviesInGame-Show.json', 'Game-Show')
  # generateDirectorsListForGenre('moviesInHorror.json', 'Horror')
  # generateDirectorsListForGenre('moviesInFamily.json', 'Family')

  pass

############################################################################################

def tsvToCsv(tsv_file):
  outputFileName = tsv_file[:-4]
  outputFileName += '.csv'
  csv_table=pd.read_table(tsv_file,sep='\t')
  csv_table.to_csv(outputFileName,index=False)

############################################################################################


def processTitleBasicsCsv():
  csv_file='title.basics.csv'
  df = pd.read_csv(
      csv_file,
      usecols=['tconst', 'titleType', 'primaryTitle', 'originalTitle', 'startYear', 'genres']
  )

  onlyMovies = df[df.titleType == 'movie']
  onlyMoviesWithGenres = onlyMovies[onlyMovies.genres != '\\N']
  onlyMoviesWithGenres = onlyMoviesWithGenres.drop(columns='titleType', axis=1)
  onlyMoviesWithGenres.to_csv('processedTitleBasics.csv',index=False)
  
############################################################################################

def processNameBasicsCsv():
  csv_file='name.basics.csv'
  df = pd.read_csv(
    csv_file,
    skiprows=1,
    names=['nconst','primaryName','birthYear','deathYear','primaryProfession','knownForTitles'],
    usecols=['nconst', 'primaryName', 'primaryProfession', 'knownForTitles']
  )

  #row[3] because row us treated as a tuple, so row[3] is not the 3rd row of the data, but the 3rd attribute, which is 'primaryProfession'
  onlyDirectors = [row for row in df.itertuples() if isinstance(row[3], str) and "director" in row[3].split(',')]

  # Below is equivalent
  # onlyDirectors = []
  # for row in df.itertuples():
  #   if (isinstance(row[3], str)):
  #     listOfProfession = row[3].split(',')
  #     for profession in listOfProfession:
  #       if 'director' == profession:
  #         onlyDirectors.append(row)

  listToDataFrame = pd.DataFrame(onlyDirectors)
  listToDataFrame = listToDataFrame.drop(columns='Index', axis=1)
  listToDataFrame.to_csv('processedNameBasics.csv', index=False)

############################################################################################

def processTitleCrewCsv():
  df1 = pd.read_csv(
    'processedTitleBasics.csv'
  )
  
  df2 = pd.read_csv(
    'title.crew.csv',
    skiprows=1,
    names=['tconst','directors','writers'],
    usecols=['tconst','directors']
  )

  listOfMovies2 = {movieId: None for movieId in df1['tconst']}


  #onlyMoviesInListOfMovies = [row for row in df2.itertuples() if row[1] in listOfMovies]
  count = 0
  totalCounter = 0
  numMovies = len(df2)
  onlyMoviesInListOfMovies = []
  for row in df2.itertuples():
    try:
      listOfMovies2[row[1]]
      onlyMoviesInListOfMovies.append(row)
      count += 1
    except KeyError:
      pass
    totalCounter += 1
    percentage = (totalCounter/numMovies)*100 # dont user len(listOfMovies) use the len of df2
    print('Number of movies in onlyMoviesInListOfMovies:', count)
    print('Number of movies checked in total:', totalCounter)
    print('Percent done: %.2f \n' % percentage)

    
    # if row[1] in listOfMovies:
    #   onlyMoviesInListOfMovies.append(row)
    #   count += 1
    # totalCounter += 1

  listToDataFrame = pd.DataFrame(onlyMoviesInListOfMovies)
  listToDataFrame = listToDataFrame.drop(columns='Index', axis=1)
  listToDataFrame.to_csv('processedTitleCrew.csv', index=False)

############################################################################################

def processTitleRatings():
  df1 = pd.read_csv(
    'processedTitleBasics.csv'
  )
  
  df2 = pd.read_csv(
    'title.ratings.csv',
    skiprows=1,
    names=['tconst','averageRating','numVotes'],
  )

  listOfMovies2 = {movieId: None for movieId in df1['tconst']}

  count = 0
  totalCounter = 0
  numMovies = len(df2)
  onlyMoviesInListOfMovies = []
  for row in df2.itertuples():
    try:
      listOfMovies2[row[1]]
      onlyMoviesInListOfMovies.append(row)
      count += 1
    except KeyError:
      pass
    totalCounter += 1
    percentage = (totalCounter/numMovies)*100 # dont user len(listOfMovies) use the len of df2
    print('Number of movies in onlyMoviesInListOfMovies:', count)
    print('Number of movies checked in total:', totalCounter)
    print('Percent done: %.2f \n' % percentage)

  listToDataFrame = pd.DataFrame(onlyMoviesInListOfMovies)
  listToDataFrame = listToDataFrame.drop(columns='Index', axis=1)
  listToDataFrame.to_csv('processedTitleRatings.csv', index=False)


############################################################################################

def generateAListOfGenres():
  df = pd.read_csv('processedTitleBasics.csv')
  listOfGenres = []

  for genreList in df.genres:
    genreList = genreList.split(',')
    for genre in genreList:
      listOfGenres.append(genre)

  listOfGenres = set(listOfGenres)

  with open('listOfGenres.txt', 'w') as f:
    for genre in listOfGenres:
        f.write("%s\n" % genre)

############################################################################################

def getAllMoviesInGenre():
  
  readFileContents = ''

  with open('listOfGenres.txt', 'r') as f:
    readFileContents = f.readlines()
    
  genres = [x.strip() for x in readFileContents]
  
  for genre in genres:
    solrContents = urllib.request.urlopen(f"http://localhost:8983/solr/title-basics/select?df=genres&fl=tconst&q={genre}&rows=2147483647")
    with open(f'moviesIn{genre}.json', 'wb') as f:
      shutil.copyfileobj(solrContents, f)

############################################################################################

def generateMovieJsonInGenre(genre):
    contents = requests.get(f"http://localhost:8983/solr/title-basics/select?df=genres&fl=tconst&q={genre}&rows=2147483647")
    listOfMovies = contents.json()
    arrayOfMovieIDsInGenre = []

    for i in listOfMovies["response"]["docs"]:
        arrayOfMovieIDsInGenre.append(i["tconst"][0])
    
    return arrayOfMovieIDsInGenre

############################################################################################

def generateDirectorsListForGenre(csv_file, genre):
  listOfTotalDirectors = []
  df = pd.read_json(f'./moviesInGenre/{csv_file}')

  count = 1
  totalCount = df["response"]["numFound"]

  for movie in df["response"]["docs"]:
    contents = requests.get(f"http://localhost:8983/solr/title-crew/select?df=tconst&fl=directors&q={movie['tconst'][0]}")
    directorsForMovie = contents.json()

    print(movie['tconst'][0])
    if directorsForMovie["response"]["numFound"] > 0 and directorsForMovie["response"]["docs"][0]["directors"] != '\\N':
      for directorID in directorsForMovie["response"]["docs"][0]["directors"]:
        listOfTotalDirectors.append(directorID)
    percent = (count/totalCount)*100
    count += 1
    print(f"Percent done for {genre}: {percent}")

  with open(f'./directorsInGenre/listOfDirectorsIn{genre}.txt', 'w') as f:
    for director in listOfTotalDirectors:
      f.write("%s\n" % director)      


############################################################################################

def generateAJsonOfMoviesForAllDirectors():
  csv_file1='processedNameBasics.csv'
  df1 = pd.read_csv(
      csv_file1,
      usecols=['nconst']
  )

  moviesOfAllDirectors = {}

  count = 0
  totalCount = len(df1)
  for director in df1.itertuples():
    directorID = director[1]
    contents = requests.get(f"http://localhost:8983/solr/title-crew/select?df=directors&fl=tconst&q={directorID}&rows=2147483647")
    moviesOfDirector = contents.json()
    arrayOfMovies = []
    if(moviesOfDirector["response"]["numFound"] > 0):
      for i in moviesOfDirector["response"]["docs"]:
        arrayOfMovies.append(i["tconst"][0])
    moviesOfAllDirectors[director[1]] = arrayOfMovies
    count += 1
    percent = (count/totalCount)*100
    print("Percent Done:", percent)

  with open('moviesOfAllDirectors.json', 'w') as f:
    json.dump(moviesOfAllDirectors, f)

############################################################################################

def readMoviesOfAllDirectorsJson():    
  with open('moviesOfAllDirectors.json') as f:
    data = json.load(f)

  for director in data:
    print(data[director])

############################################################################################

def generateJsonAverageRatingForEachDirector():

  dictOfDirectorMovieRatings = {}

  with open('moviesOfAllDirectors.json') as f:
    data = json.load(f)

  count = 1
  totalCount = len(data)
  for director in data:
    ratingsArray = []
    for movieID in data[director]:
      contents = requests.get(f"http://localhost:8983/solr/title-ratings/select?df=tconst&fl=averageRating&q={movieID}")
      rating = contents.json()
      if rating["response"]["numFound"] > 0:
        ratingsArray.append(rating["response"]["docs"][0]["averageRating"][0])
    if len(ratingsArray) > 0:
      averageRating = mean(ratingsArray)
    else:
      averageRating = 0
    dictOfDirectorMovieRatings[director] = averageRating
    percent = (count/totalCount)*100
    print("Percent Done:", percent)
    count += 1

  with open('directorAverageRatings.json', 'w') as f:
    json.dump(dictOfDirectorMovieRatings, f)

############################################################################################


if __name__== "__main__":
  main()

'''
References:

https://www.datacamp.com/community/tutorials/pandas-read-csv

https://data36.com/pandas-tutorial-1-basics-reading-data-files-dataframes-data-selection/

https://stackoverflow.com/questions/18039057/python-pandas-error-tokenizing-data

https://stackoverflow.com/questions/7837722/what-is-the-most-efficient-way-to-loop-through-dataframes-with-pandas

https://stackoverflow.com/questions/18262293/how-to-open-every-file-in-a-folder
'''