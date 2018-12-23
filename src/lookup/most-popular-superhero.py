from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularHero")
sc = SparkContext(conf = conf)

def countCoOccurences(line):
    elements = line.split()
    return (int(elements[0]), len(elements) - 1)

def parseNames(line):
    fields = line.split('\"')
    # encode returns bytes, so you need to convert '\n' to bytes as well
    return (int(fields[0]), str(fields[1].encode("utf8") + b"\n"))

names = sc.textFile("/Users/adnanbajwa/sparkcourse/marvel-names.txt")
namesRdd = names.map(parseNames)

lines = sc.textFile("/Users/adnanbajwa/sparkcourse/marvel-graph.txt")
# Count co-occurences to find out the most popular super hero on that basis.
pairings = lines.map(countCoOccurences)

# Super hero with co-occurences can span to multiple line. To handle this reduceByKey is used.
totalFriendsByCharacter = pairings.reduceByKey(lambda x, y : x + y)
flipped = totalFriendsByCharacter.map(lambda x : (x[1], x[0]))

mostPopular = flipped.max()

mostPopularName = namesRdd.lookup(mostPopular[1])[0]

print(mostPopularName + " is the most popular superhero, with " + str(mostPopular[0]) + " co-appearances.")
