from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("SparkStreamingApp") \
    .getOrCreate()

# Initialize Spark Streaming Context with a batch interval of 1 second
ssc = StreamingContext(spark.sparkContext, 1)

# Example: Define a socket stream (replace with your source)
lines = ssc.socketTextStream("localhost", 9999)

# Example: Perform a simple transformation
words = lines.flatMap(lambda line: line.split(" "))
word_counts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

# Print the results
word_counts.pprint()

# Start the streaming computation
ssc.start()

# Wait for the streaming to finish
ssc.awaitTermination()