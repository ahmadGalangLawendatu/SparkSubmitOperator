from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("WordCount").getOrCreate()

# Read the text file
text_file = spark.read.text("./opt/SparkSubmitOperator/book.txt")

# Split the text into words and count the occurrences of each word
word_counts = text_file.selectExpr("explode(split(value, ' ')) as word").groupBy("word").count()

# Show the word counts
word_counts.show()

# Stop the Spark session
spark.stop()
