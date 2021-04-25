import os

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

sc = SparkContext("local", "lab6i")
spark = SparkSession(sc)
weatherDF = spark.read.option("multiLine", "true").json("weather.json")
weatherDF.printSchema()


# routine to flat columns of the dataframe.
def read_nested_json(df):
    column_list = []
    for column_name in df.schema.names:
        if isinstance(df.schema[column_name].dataType, ArrayType):
            df = df.withColumn(column_name, explode(column_name).alias(column_name))
            column_list.append(column_name)
        elif isinstance(df.schema[column_name].dataType, StructType):
            for field in df.schema[column_name].dataType.fields:
                column_list.append(col(column_name + "." + field.name).alias(column_name + "_" + field.name))
        else:
            column_list.append(column_name)
    df = df.select(column_list)
    return df


is_all_columns_flattened = False
while not is_all_columns_flattened:
    # existing columns are flattened and appended to the schema columns
    weatherDF = read_nested_json(weatherDF)
    is_all_columns_flattened = True
    # check till all new columns are flattened
    for column_name in weatherDF.schema.names:
        if isinstance(weatherDF.schema[column_name].dataType, ArrayType):
            is_all_columns_flattened = False
        elif isinstance(weatherDF.schema[column_name].dataType, StructType):
            is_all_columns_flattened = False
weatherDF.printSchema()
weatherDF.show()

weatherDF.createTempView("halifax_weather_filter")
tempdf = spark.sql("select daily_dt, daily_feels_like_day from halifax_weather_filter where daily_feels_like_day < 5.0")
tempdf.show()
# ref: https://stackoverflow.com/questions/43269244/pyspark-dataframe-write-to-single-json-file-with-specific-name
tempdf.coalesce(1).write.format('json').save('op')
os.system("cat op/*.json > cold_weather.json")
os.system("rm -rf op")
