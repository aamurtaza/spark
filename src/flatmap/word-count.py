from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("/Users/adnanbajwa/sparkcourse/book.txt")
words = input.flatMap(lambda x: x.split()) # Go through each line and split words separated by whitespace. 
wordCounts = words.countByValue()

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore') # This takes care of encoding issues, to make words printable
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
