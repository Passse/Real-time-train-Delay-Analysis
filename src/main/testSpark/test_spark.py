from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

spark = (
    SparkSession.builder
    .appName("SparkTest")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

data = [
    ("London Euston", 5),
    ("London Euston", 12),
    ("Manchester Piccadilly", 3),
]

df = spark.createDataFrame(data, ["station", "delay_minutes"])

result = df.groupBy("station").avg("delay_minutes")

result.show()

spark.stop()