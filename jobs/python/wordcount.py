from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("PythonWordCount").getOrCreate()

# Sample text
text = "Hello Spark Hello Python Hello Airflow Hello Docker and Hello Galang"

# Parallelize the text into an RDD
words = spark.sparkContext.parallelize(text.split(" "))

# Perform word count
wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

# Collect and print the results
for wc in wordCounts.collect():
    print(wc[0], wc[1])

# Stop the Spark session
spark.stop()
