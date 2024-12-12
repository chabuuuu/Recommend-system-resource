from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALSModel
import csv

def loadMovieNames():
    movieID_to_name = {}
    with open("/home/haphuthinh/Workplace/School_project/do-an-1/Recommender-system/ml-20m/movies.csv", newline='', encoding='ISO-8859-1') as csvfile:
        movieReader = csv.reader(csvfile)
        next(movieReader)  # Skip header line
        for row in movieReader:
            movieID = int(row[0])
            movieName = row[1]
            movieID_to_name[movieID] = movieName
    return movieID_to_name

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("ALSRecommendation")\
        .config("spark.driver.memory", "10g") \
        .config("spark.executor.cores", '6')\
        .getOrCreate()

    # Tải mô hình đã lưu
    model = ALSModel.load("/home/haphuthinh/Workplace/School_project/do-an-1/Recommender-system/TrainedModel/ALSModel")

    # Lấy dữ liệu movie names để hiển thị
    movieID_to_name = loadMovieNames()

    # Gợi ý cho tất cả người dùng
    userRecs = model.recommendForAllUsers(10)

    # Lọc gợi ý cho user cụ thể
    user85Recs = userRecs.filter(userRecs['userId'] == 100).collect()

    # Hiển thị các gợi ý cho user cụ thể
    for row in user85Recs:
        print(f"Recommendations for User {row['userId']}:")
        for rec in row.recommendations:
            if rec.movieId in movieID_to_name:
                print(f"- {movieID_to_name[rec.movieId]} (Rating: {rec.rating:.2f})")

    spark.stop()
