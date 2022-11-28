# chapter2 예제
./data/mnm_dataset.csv 데이터를 읽어와 각 주마다 mnm 색상이 몇개씩 있는 지 count하는 예제

```sh
$ ${SPARK_HOME}/bin/spark-submit mmcount.py data/mnm_dataset.csv
```