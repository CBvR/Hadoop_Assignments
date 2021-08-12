from mrjob.job import MRJob
from mrjob.step import MRStep

class Ratings(MRJob):
    def steps(self):
		#Does the MRSteps
        return [
            MRStep(
				mapper=self.mapper_get_all_movies, 
				combiner=self.combiner_get_count_ratings_by_movies, 
				reducer=self.reducer_sum_up_rating_counts_from_movies
			),
            MRStep(
				reducer=self.reducer_sort_all_movies_by_ratings
				)
        ]

    #Split the text on \t and get the movie_id 
    def mapper_get_all_movies(self, _, line):
        (_, movie_id, _, _) = line.split('\t')
        yield movie_id, 1
    
    #This will combine the ratings count with their corresponding movie ids
    def combiner_get_count_ratings_by_movies(self, movie_id, ratings):
        yield movie_id, sum(ratings)
    
    #This will sum up the the count of the ratings corresponding to the movie_id
    def reducer_sum_up_rating_counts_from_movies(self, movie_id, ratings):
        yield None, (sum(ratings), movie_id)

    #This will sort the movies based on the ratings the movie has
    def reducer_sort_all_movies_by_ratings(self, _, movies):
        for count, movie_id in sorted(movies):
            yield (int(movie_id), int(count))

if __name__ == "__main__":
    Ratings.run()