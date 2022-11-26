import sys
from pyspark.sql import SparkSession

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: mmcount <file>", file=sys.stderr)
        sys.exit(-1)

    # SparkSession API를 써서 SparkSession 객체를 만든다.
    # 존재하지 않으면 객체를 생성한다.
    # JVM 마다 SparkSession 객체는 하나만 존재할 수 있다.
    spark = (SparkSession
             .builder
             .appName("PythonMnMCount")
             .getOrCreate())

    # 명령행 인자에서 M&M 데이터가 들어있는 파일 이름을 얻는다
    mnm_file = sys.argv[1]

    # 스키마 추론과 쉼표로 구분된 칼럼 이름이 제공되는 헤더가 있음을 지정햊고 CSV 포맷으로 파일을 읽어들여 데이터 프레임에 저장한다.
    mnm_df = (spark.read.format("csv")
              .option("header", "true")
              .option("inferSchema", "true")
              .load(mnm_file))

    # 데이터 프레임 고수준 API를 사용하고 RDD는 전혀 사용하지 않고 있음
    # 일부 스파크 함수들은 동일한 객체를 되돌려주므로 함수 호출을 체이닝할 수 있다.
    # 1. 데이터 프레임에서 "State", "Color", "Count" 필드를 읽는다
    # 2. 각 주별로 색깔별로 그룹화하기 위해 groupBy()를 사용한다
    # 3. 그룹화된 주/색깔별로 모든 색깔별 집계를 한다.
    # 4. 역순으로 orderBy() 한다.
    count_mnm_df = (mnm_df
                    .select("State", "Color", "Count")
                    .groupby("State", "Color")
                    .sum("Count")
                    .orderBy("sum(Count)", ascending=False))

    # 모든 주와 색깔별로 결과를 보여준다.
    # show()는 액션이므로 위의 쿼리 내용들이 시작되게 된다.
    count_mnm_df.show(n=60, truncate=False)
    print("Total Rows = %d" % (count_mnm_df.count()))

    # 위 코드는 모든 주에 대한 집계를 계산한 것이다.
    # 특정 주(CA)에 대한 데이터만 보기 원한다면 아래와 같은 과정을 거친다.
    # 1. 데이터 프레임에서 모든 줄을 읽는다
    # 2. 원하는 주(CA)에 대한 것만 걸러낸다.
    # 3. 위에서 했던 것처럼 주와 색깔별로 groupBy() 한다.
    # 4. 각 색깔별로 카운트를 합친다.
    # 5. orderBy()로 역순 정렬.
    ca_count_mnm_df = (mnm_df
                       .select("State", "Color", "Count")
                       .where(mnm_df.State == "CA")
                       .groupby("State", "Color")
                       .sum("Count")
                       .orderBy("sum(Count)", ascending=False))

    # 캘리포니아의 집계 결과를 보여준다.
    # show()를 통해 전체 연산 실행을 발동하는 액션을 준다.
    ca_count_mnm_df.show(n=10, truncate=False)

    # SparkSession을 멈춘다.
    spark.stop()
