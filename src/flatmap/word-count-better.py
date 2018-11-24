import re
from pyspark import SparkConf, SparkContext


"""
    Go through each line and split words based on word using regular expression e.g. avoid punctuation etc.
    Then transform them into lower case.
"""
def normalizeWords(text): 
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)
        
input = sc.textFile("/Users/adnanbajwa/sparkcourse/book.txt")
words = input.flatMap(normalizeWords) 
wordCounts = words.countByValue()

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore') # This takes care of encoding issues, to make words printable
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
