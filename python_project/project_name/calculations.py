import argparse

import pandas as pd
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("PySpark") \
    .getOrCreate()

def calculations(input_path, input_year):
    
    df = pd.read_csv(input_path, parse_dates=['dt'])
    sdf = spark.createDataFrame(df)
    sdf = (sdf
       .withColumn('year', F.year('dt'))
       .withColumn('month', F.month('dt'))
       .filter(F.col('year') == int(input_year))
       .groupBy(['City', 'month']).agg(F.mean(F.col('AverageTemperature')).alias('AverageTemperature'))
       .orderBy(F.col('AverageTemperature').desc())
    )
    warmth = sdf.toPandas()

    return warmth['month'].values[0], warmth['City'].values[0]


def main():
    parser = argparse.ArgumentParser(description='')
    parser.add_argument(type=str, help='', dest='a')
    parser.add_argument(type=int, help='', dest='b')
    args = parser.parse_args()
    input_path, input_year = args.a, args.b

    print(calculations(input_path, input_year))


if __name__ == "__main__":
    main()
