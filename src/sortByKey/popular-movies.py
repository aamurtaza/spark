from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularMovies")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split()
    return (int(fields[1]),int(fields[2]))

lines = sc.textFile("/Users/adnanbajwa/sparkcourse/ml-100k/u.data")

# Top Movie on the basis of count
movies = lines.map(lambda x: (int(x.split()[1]), 1))
moviesCount = movies.reduceByKey(lambda x, y: x + y)
flipped = moviesCount.map(lambda x: (x[1], x[0]))
sortedMovies = flipped.sortByKey()
results = sortedMovies.collect()


# Top Movie on the basis of average rating given by users
# movieRatingPair = lines.map(parseLine)
# ratingsCountPerMovie = movieRatingPair.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
# averageRatingPerMovie = ratingsCountPerMovie.mapValues(lambda x: x[0] / x[1])
# # sortedByAvgRating = averageRatingPerItem.sortBy(lambda x: x[1])
# flipped = averageRatingPerMovie.map(lambda x: (x[1], x[0]))
# sortedByAvgRating = flipped.sortByKey()
# results = sortedByAvgRating.collect()

for result in results:
    print(result)
