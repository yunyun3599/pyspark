from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

if __name__ == "__main__":

    # SparkSession으로부터 데이터 프레임을 만든다.
    spark = (SparkSession
             .builder
             .appName("AuthorsAges")
             .getOrCreate())

    # 데이터 프레임 생성
    data_df = spark.createDataFrame(data=[("Brooke", 20), ("Denny", 31), ("Jules", 30), ("TD", 35), ("Brooke", 25)],
                                    schema=["name", "age"])

    # 동일한 이름으로 그룹화하여 나이별로 계산해 평균을 구한다.
    avg_df = data_df.groupby("name").agg(avg("age"))
    avg_df.show()
