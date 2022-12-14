from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = (SparkSession
             .builder
             .appName("SchemaExample")
             .getOrCreate())

    fire_schema = StructType([StructField('CallNumber', IntegerType(), True),
                              StructField('UnitID', StringType(), True),
                              StructField("IncidentNumber", IntegerType(), True),
                              StructField("CallType", StringType(), True),
                              StructField("CallDate", StringType(), True),
                              StructField("WatchDate", StringType(), True),
                              StructField("CallFinalDisposition", StringType(), True),
                              StructField("AvailableDtTm", StringType(), True),
                              StructField("Address", StringType(), True),
                              StructField("City", StringType(), True),
                              StructField("Zipcode", IntegerType(), True),
                              StructField("Battalion", StringType(), True),
                              StructField("StationArea", StringType(), True),
                              StructField("Box", StringType(), True),
                              StructField("OriginalPriority", StringType(), True),
                              StructField("Priority", StringType(), True),
                              StructField("FinalPriority", IntegerType(), True),
                              StructField("ALSUnit", BooleanType(), True),
                              StructField("CallTypeGroup", StringType(), True),
                              StructField("NumAlarms", IntegerType(), True),
                              StructField("UnitType", StringType(), True),
                              StructField("UnitSequenceInCallDispatch", IntegerType(), True),
                              StructField("FirePreventionDistrict", StringType(), True),
                              StructField("SupervisorDistrict", StringType(), True),
                              StructField("Neighborhood", StringType(), True),
                              StructField("Location", StringType(), True),
                              StructField("RowID", StringType(), True),
                              StructField("Delay", FloatType(), True),
                              ])

    # DataFrameReader ?????????????????? CSV ????????? ??????
    sf_fire_file = "./data/sf-fire-calls.csv"
    fire_df = spark.read.csv(sf_fire_file, header=True, schema=fire_schema)

    print(fire_df.show())

    parquet_path = "/Users/yoonjae/study/pyspark/ch3/data/result"
    # fire_df.write.format("parquet").save(parquet_path)

    # ????????????, ??????
    few_fire_df = (fire_df
                   .select("IncidentNumber", "AvailableDtTm", "CallType")
                   .where(col("CallType") != "Medical Incident"))
    few_fire_df.show(5, truncate=False)

    # countDistinct()??? ?????? ?????? ????????? ????????? ???????????????
    fire_df\
        .select("CallType")\
        .where(col("CallType").isNotNull())\
        .agg(countDistinct("CallType").alias("DistinctCallTypes"))\
        .show()

    # null??? ?????? ?????? CallType ??????
    fire_df\
        .select("CallType")\
        .where(col("CallType").isNotNull())\
        .distinct()\
        .show(10, False)

    # ????????? ?????? ??????
    new_fire_df = fire_df.withColumnRenamed("Delay", "ResponseDelayedinMins")
    (new_fire_df
     .select("ResponseDelayedinMins")
     .where(col("ResponseDelayedinMins") > 5)
     .show(5, False))

    # String?????? ???????????? date??? timestamp?????? ?????? ????????? ??????
    fire_ts_df = (new_fire_df
                  .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
                  .drop("CallDate")
                  .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
                  .drop("WatchDate")
                  .withColumn("AvailableDtTs", to_timestamp(col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a"))
                  .drop("AvailableDtTm"))
    (fire_ts_df
     .select("IncidentDate", "OnWatchDate", "AvailableDtTs")
     .show(5, False))
    # ??? ?????? ????????? timestamp ?????? ????????? ??? ?????? ?????? ?????? ?????? - dayofmonth(), dayofyear(), dayofweek() ???

    # ????????? ????????? ??? ??? ????????? ????????? ????????? ???????????? ????????? ?????? ??????
    (fire_ts_df
     .select(year('IncidentDate'))
     .distinct()
     .orderBy(year('IncidentDate'))
     .show())

    # ?????? ??????
    # ?????? ?????? ?????? ??????
    (fire_ts_df
     .select("CallType")
     .where(col("CallType").isNotNull())
     .groupBy("CallType")
     .count()
     .orderBy("count", ascending=False)
     .show(10, truncate=False))

    # ??? ?????? ???????????? ?????????
    (fire_ts_df
     .select(sum("NumAlarms"), avg("ResponseDelayedinMins"), min("ResponseDelayedinMins"), max("ResponseDelayedinMins"))
     .show())
