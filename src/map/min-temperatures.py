from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

def g(x):
    print(x)

def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0 # Temperature in Fahrenheit
    return (stationID, entryType, temperature)

lines = sc.textFile("/Users/adnanbajwa/sparkcourse/1800.csv")
parsedLines = lines.map(parseLine)
minTemps = parsedLines.filter(lambda x: "TMIN" in x[1]) # Filter entries which have TMIN entry e.g. ('ITE00100554', 'TMIN', 5.359999999999999)
stationTemps = minTemps.map(lambda x: (x[0], x[2]))     # Create key-value pair of (stationId, Temperature) e.g. ('ITE00100554', 5.359999999999999)
# stationTemps.foreach(g)
minTemps = stationTemps.reduceByKey(lambda x, y: min(x,y)) # ReduceByKey,which is basically stationId and min() make sure to pick up min value between two values. 
results = minTemps.collect();
for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))
