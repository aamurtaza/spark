from pyspark import SparkConf, SparkContext

def parseLine(line):
    fields = line.split()
    return (int(fields[1]),int(fields[2]))

def loadMovieNames():
    movieNames = {}
    # specifying 'latin-1' encoding to prevent UniCodeDecoderError
    with open("ml-100k/u.ITEM", encoding ='latin-1') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

conf = SparkConf().setMaster("local").setAppName("PopularMoviesWithNames")
sc = SparkContext(conf = conf)

nameDict = sc.broadcast(loadMovieNames())

lines = sc.textFile("/Users/adnanbajwa/sparkcourse/ml-100k/u.data")

# Top Movie on the basis of count
movies = lines.map(lambda x: (int(x.split()[1]), 1))
moviesCount = movies.reduceByKey(lambda x, y: x + y)
flipped = moviesCount.map(lambda x: (x[1], x[0]))
sortedMovies = flipped.sortByKey()
sortedMoviesWithNames = sortedMovies.map(lambda countMovie : (nameDict.value[countMovie[1]], countMovie[0]))

# Top Movie on the basis of average rating given by users
# movieRatingPair = lines.map(parseLine)
# ratingsCountPerMovie = movieRatingPair.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
# averageRatingPerMovie = ratingsCountPerMovie.mapValues(lambda x: x[0] / x[1])
# # sortedByAvgRating = averageRatingPerItem.sortBy(lambda x: x[1])
# flipped = averageRatingPerMovie.map(lambda x: (x[1], x[0]))
# sortedByAvgRating = flipped.sortByKey()
# sortedMoviesWithNames = sortedByAvgRating.map(lambda movieWithRating : (nameDict.value[movieWithRating[1]], movieWithRating[0]))

results = sortedMoviesWithNames.collect()

for result in results:
    print (result)
