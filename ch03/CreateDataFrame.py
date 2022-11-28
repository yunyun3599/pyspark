from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# 스파크 데이터 프레임 API 사용하여 스키마 정의
schema = StructType([StructField("author", StringType(), False),
                     StructField("title", StringType(), False),
                     StructField("pages", IntegerType(), False)])

# DDL 써서 스키마 정의
schema2 = "author STRING, title STRING, pages INT"

if __name__ == "__main__":

    # DDL 써서 스키마 정의
    schema = "Id INT, First STRING, Last STRING, Url STRING, " \
             "Published STRING, Hits INT, Campaigns ARRAY<STRING>"

    data = [[1, "Jules", "Damji", "https://tinyurl.1", "1/4/2016", 4535, ["twitter", "LinkedIn"]],
            [2, "Brooke", "Wenig", "https://tinyurl.2", "5/5/2018", 8908, ["twitter", "LinkedIn"]],
            [3, "Denny", "Lee", "https://tinyurl.3", "6/7/2019", 7659, ["web", "twitter", "FB", "LinkedIn"]],
            [4, "Tathagata", "Das", "https://tinyurl.4", "5/12/2018", 10568, ["twitter", "FB"]],
            [5, "Matei", "Zaharia", "https://tinyurl.5", "5/14/2014", 40578, ["web", "twitter", "FB", "LinkedIn"]],
            [6, "Reynold", "Xin", "https://tinyurl.6", "3/2/2015", 25568, ["twitter", "LinkedIn"]]
            ]

    # SparkSession 생성
    spark = (SparkSession
             .builder
             .appName("SchemaExample")
             .getOrCreate())

    # 위에서 정의한 스키마로 데이터 프레임 생성
    blogs_df = spark.createDataFrame(data, schema)
    # 데이터 프레임 내용 보여주기
    blogs_df.show()
    # 데이터 프레임 처리에 사용된 스키마 출력
    print(blogs_df.printSchema())
    print(blogs_df.schema)

    # DataFrame column과 expresssion
    print(blogs_df.columns)

    # 값 계산을 위한 표현식 사용
    blogs_df.select(expr("Hits * 2")).show(2)

    # col을 이용한 계산
    blogs_df.select(col("Hits") * 2).show(2)

    # 블로그 우수 방문자를 계산하기 위한 식 표현
    print("### Hits가 10,000 이상인 경우 True 값을 갖는 Big Hitters 컬럼 추가")
    blogs_df.withColumn("Big Hitters", (expr("Hits > 10000"))).show()

    blogs_df.withColumn("AuthorsId", (concat(expr("First"), expr("Last"), expr("Id"))))\
        .select(col("AuthorsId"))\
        .show(4)

    # 동일한 값을 리턴하는 다른 표현들
    blogs_df.select(expr("Hits")).show(2)
    blogs_df.select(col("Hits")).show(2)
    blogs_df.select("Hits").show(2)

    # Id 칼럼 값에 따라 역순 정렬
    blogs_df.sort(col("Id").desc()).show()
