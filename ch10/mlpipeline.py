from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = (SparkSession
             .builder
             .appName("MLExample")
             .getOrCreate())

    filepath = "./data/sf-airbnb-clean.parquet"
    airbnbDF = spark.read.parquet(filepath)
    airbnbDF\
        .select("neighbourhood_cleansed", "room_type", "bedrooms", "bathrooms", "number_of_reviews", "price")\
        .show(5)

    trainDF, testDF = airbnbDF.randomSplit([.8, .2], seed=42)
    print(f"There are {trainDF.count()} rows in the training set, and {testDF.count()} in the test set")

    # 단일 벡터로 변환
    from pyspark.ml.feature import VectorAssembler
    vecAssembler = VectorAssembler(inputCols=["bedrooms"], outputCol="features")
    vecTrainDF = vecAssembler.transform(trainDF)
    vecTrainDF.select("bedrooms", "features", "price").show(10)

    # 모델 구축
    from pyspark.ml.regression import LinearRegression
    lr = LinearRegression(featuresCol="features", labelCol="price")
    lrModel = lr.fit(vecTrainDF)

    m = round(lrModel.coefficients[0], 2)
    b = round(lrModel.intercept, 2)
    print(f"""The formula for the linear regression line is 
    price = {m} * bedrooms + {b}""")

    # 파이프라인 생성
    from pyspark.ml import Pipeline
    pipeline = Pipeline(stages=[vecAssembler, lr])
    pipelineModel = pipeline.fit(trainDF)

    predDF = pipelineModel.transform(testDF)
    predDF.select("bedrooms", "features", "price", "prediction").show(10)

    # 원 핫 인코딩
    from pyspark.ml.feature import OneHotEncoder, StringIndexer

    categoricalCols = [field for (field, dataType) in trainDF.dtypes if dataType == "string"]
    indexOutputCols = [x + "Index" for x in categoricalCols]
    oheOutputCols = [x + "OHE" for x in categoricalCols]

    stringIndexer = StringIndexer(inputCols=categoricalCols,
                                  outputCols=indexOutputCols,
                                  handleInvalid="skip")
    print("#######StringIndexer")
    strDF = stringIndexer.fit(trainDF).transform(trainDF)
    strDF.select(indexOutputCols).show(5)

    print("#######OHEEncoder")
    oheEncoder = OneHotEncoder(inputCols=indexOutputCols,
                               outputCols=oheOutputCols)
    oheEncoder.fit(strDF).transform(strDF).select(oheOutputCols).show(5)

    numericCols = [field for (field, dataType) in trainDF.dtypes
                   if ((dataType == "double") & (field != "price"))]
    assemblerInputs = oheOutputCols + numericCols
    vecAssembler = VectorAssembler(inputCols=assemblerInputs,
                                   outputCol="features")

    lr = LinearRegression(labelCol="price", featuresCol="features")
    pipeline = Pipeline(stages=[stringIndexer, oheEncoder, vecAssembler, lr])
    pipelineModel = pipeline.fit(trainDF)
    predDF = pipelineModel.transform(testDF)
    predDF.select("features", "price", "prediction").show(5)
