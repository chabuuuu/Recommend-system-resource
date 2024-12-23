# -*- coding: utf-8 -*-
"""
Created on Mon May 28 11:09:55 2018

@author: Frank
"""

from pyspark.sql import SparkSession

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row

import csv

def loadMovieNames():
    movieID_to_name = {}
    with open("/home/haphuthinh/Workplace/School_project/do-an-1/Recommender-system/ml-20m/movies.csv", newline='', encoding='ISO-8859-1') as csvfile:
        movieReader = csv.reader(csvfile)
        next(movieReader)  #Skip header line
        for row in movieReader:
            movieID = int(row[0])
            movieName = row[1]
            movieID_to_name[movieID] = movieName
    return movieID_to_name

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("ALSExample")\
        .config("spark.driver.memory", "6g") \
        .config("spark.executor.cores", '5')\
        .getOrCreate()

    lines = spark.read.option("header", "true").csv("/home/haphuthinh/Workplace/School_project/do-an-1/Recommender-system/ml-20m/ratings.csv").rdd

    ratingsRDD = lines.map(lambda p: Row(userId=int(p[0]), movieId=int(p[1]),
                                         rating=float(p[2]), timestamp=int(p[3])))
    
    ratings = spark.createDataFrame(ratingsRDD)
    
    (training, test) = ratings.randomSplit([0.8, 0.2])

    als = ALS(maxIter=5, regParam=0.01, userCol="userId", itemCol="movieId", ratingCol="rating",
              coldStartStrategy="drop")
    model = als.fit(training)

    model.save("/home/haphuthinh/Workplace/School_project/do-an-1/Recommender-system/TrainedModel/ALSModel")


    predictions = model.transform(test)
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                    predictionCol="prediction")
    rmse = evaluator.evaluate(predictions)
    print("Root-mean-square error = " + str(rmse))

    userRecs = model.recommendForAllUsers(10)
    
    user85Recs = userRecs.filter(userRecs['userId'] == 690).collect()
    
    spark.stop()

    movieID_to_name = loadMovieNames()
        
    for row in user85Recs:
        for rec in row.recommendations:
            if rec.movieId in movieID_to_name:
                print(movieID_to_name[rec.movieId])

