from pyspark.sql import SparkSession, Row

if __name__ == "__main__":
    spark = (SparkSession
             .builder
             .appName("Row")
             .getOrCreate())

    blog_row = Row(6, "Reynold", "Xin", "https://tinyurl.6", "3/2/2015", 25568, ["twitter", "LinkedIn"])

    # 인덱스로 개별 아이템에 접근
    print(blog_row[1])

    # Row 객체들은 빠른 탐색을 위해 데이터 프레임으로 만들어 사용하기도 함
    rows = [Row("Matei Zaharia", "CA"), Row("Reynold Xin", "CA")]
    author_df = spark.createDataFrame(rows, ["Authors", "State"])
    author_df.show()
    