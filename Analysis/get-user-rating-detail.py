import pandas as pd

# Đọc các file CSV vào DataFrame
movies_df = pd.read_csv('../ml-latest-small/movies.csv')
links_df = pd.read_csv('../ml-latest-small/links.csv')
ratings_df = pd.read_csv('../ml-latest-small/ratings.csv')

# Hàm lấy thông tin đánh giá của một user
def get_user_movie_ratings(user_id):
    # Lọc ra các đánh giá của user_id
    user_ratings = ratings_df[ratings_df['userId'] == user_id]
    
    # Kết hợp với bảng movies để lấy thông tin chi tiết về phim
    movie_details = pd.merge(user_ratings, movies_df, on='movieId', how='left')
    
    # Kết hợp thêm với bảng links để lấy IMDb, TMDB ID
    movie_details = pd.merge(movie_details, links_df, on='movieId', how='left')

    # Chọn các cột cần thiết để hiển thị
    result = movie_details[['movieId', 'title', 'genres', 'rating', 'imdbId', 'tmdbId']]
    
    return result

# Thử truy vấn với một user_id bất kỳ
user_id = 690  # Thay thế bằng userId bạn muốn kiểm tra
user_ratings = get_user_movie_ratings(user_id)

# Sắp xếp kết quả theo rating giảm dần
sorted_user_ratings = user_ratings.sort_values(by='rating', ascending=False)

# Xuất ra file CSV
sorted_user_ratings.to_csv('user_movie_ratings_sorted.csv', index=False)

# Hiển thị kết quả (nếu cần)
print(sorted_user_ratings)
