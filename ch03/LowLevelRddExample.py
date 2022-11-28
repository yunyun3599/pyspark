import pyspark

if __name__ == "__main__":
    sc = pyspark.SparkContext(appName="LowLevelRddExample")

    # (name, age) 형태의 튜플로 된 RDD를 생성한다.
    dataRDD = sc.parallelize([("Brooke", 20), ("Denny", 31), ("Jules", 30), ("TD", 35), ("Brooke", 25)])

    # 집계와 평균을 위한 람다 표현식과 함께 map과 reduceByKey 트랜스포메이션을 사용한다.
    agesRDD = (dataRDD
               .map(lambda x: (x[0], (x[1], 1)))
               .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
               .map(lambda x: (x[0], x[1][0]/x[1][1])))

    for row in agesRDD.collect():
        print(f"{row[0]}, {row[1]}")
