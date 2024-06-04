import sys
from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("CountAsBs") \
    .getOrCreate()

# Get the second argument passed to spark-submit (the first is the python app)
if len(sys.argv) != 2:
    print("Usage: spark-submit <script> <logfile>")
    sys.exit(-1)

logFile = sys.argv[1]

# Read file
logData = spark.read.text(logFile).rdd.map(lambda r: r[0])

# Get lines with 'a'
numAs = logData.filter(lambda s: 'a' in s).count()

# Get lines with 'b'
numBs = logData.filter(lambda s: 'b' in s).count()

print("Lines with a: {} , Lines with b: {}".format(numAs, numBs))

# Stop the Spark session
spark.stop()
