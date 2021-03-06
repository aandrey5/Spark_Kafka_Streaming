# /spark2.4/bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5,com.datastax.spark:spark-cassandra-connector_2.11:2.4.2               --driver-memory 512m --driver-cores 1 --master local[1]

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.regression import LinearRegression
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName("mllib_prepare_and_train_app").master("local[*]").getOrCreate()

# -- Input params
data_path = "ml_data/mumbai.csv"
data_path_json = 'ml_data/flats_mumbai.json'
model_dir = "ml_data/models"


# -- read data from storage
data = spark\
    .read\
    .format("csv")\
    .options(inferSchema=True, header=True) \
    .load(data_path)


# Cast Data Types for all worked columns

data = data.withColumn("area", data["area"].cast(IntegerType()))
data = data.withColumn("latitude", data["latitude"].cast(IntegerType()))
data = data.withColumn("longitude", data["longitude"].cast(IntegerType()))
data = data.withColumn("Bedrooms", data["Bedrooms"].cast(IntegerType()))
data = data.withColumn("Bathrooms", data["Bathrooms"].cast(IntegerType()))
data = data.withColumn("Balcony", data["Balcony"].cast(IntegerType()))
data = data.withColumn("Lift", data["Lift"].cast(IntegerType()))
data = data.withColumn("price", data["price"].cast(IntegerType()))

# Check types
print(data.schema.json())

# Remove nulls , change to "0"
data = data.fillna(0)
data = data.filter((data.latitude != 0) & (data.Bedrooms != 0))

data.show(3)
print(data.schema.json())

# target
target = ["price"]

# model evaluator
evaluator = RegressionEvaluator() \
        .setMetricName("rmse") \
        .setLabelCol("label") \
        .setPredictionCol("prediction")


# PREPARE AND TRAIN - VERSION 1 #
# ============================= #
def prepare_data_1(data, features, target):
    # features
    f_columns = ",".join(features).split(",")
    # target
    f_target = ",".join(target).split(",")
    f_target = list(map(lambda c: F.col(c).alias("label"), f_target))
    # all columns
    all_columns = ",".join(features + target).split(",")
    all_columns = list(map(lambda c: F.col(c), all_columns))
    # model data set
    model_data = data.select(all_columns)
    # preparation
    assembler = VectorAssembler(inputCols=f_columns, outputCol='features')
    model_data = assembler.transform(model_data)
    model_data = model_data.select('features', f_target[0])
    return model_data


def prepare_and_train_1(data, features, target):
    model_data = prepare_data_1(data, features, target)
    # train, test
    train, test = model_data.randomSplit([0.8, 0.2], seed=12345)
    # model
    lr = LinearRegression(featuresCol='features', labelCol='label', maxIter=10, regParam=0.01)
    # train model
    model = lr.fit(train)
    # check the model on the test data
    prediction = model.transform(test)
    prediction.show(20)
    evaluation_result = evaluator.evaluate(prediction)
    print("Evaluation result: {}".format(evaluation_result))
    return model


# PREPARE AND TRAIN - VERSION 2 #
# ============================= #
def prepare_data_2(data, features, target):
    # features
    f_columns = ",".join(features).split(",")
    # target
    f_target = ",".join(target).split(",")
    f_target = list(map(lambda c: F.col(c).alias("label"), f_target))
    # all columns
    all_columns = ",".join(features + target).split(",")
    all_columns = list(map(lambda c: F.col(c), all_columns))
    # model data set
    model_data = data.select(all_columns)
    # prepare categorical columns
    text_columns = ['Bedrooms', 'Bathrooms', 'Balcony', 'Lift']
    output_text_columns = [c + "_index" for c in text_columns]
    for c in text_columns:
        string_indexer = StringIndexer(inputCol=c, outputCol=c + '_index').setHandleInvalid("keep")
        model_data = string_indexer.fit(model_data).transform(model_data)
    print(model_data.schema.simpleString())
    model_data.show(3)
    # update f_columns with indexed text columns
    f_columns = list(filter(lambda c: c not in text_columns, f_columns))
    f_columns += output_text_columns
    # preparation
    assembler = VectorAssembler(inputCols=f_columns, outputCol='features')
    model_data = assembler.transform(model_data)
    model_data = model_data.select('features', f_target[0])
    model_data.show(3)
    return model_data


def prepare_and_train_2(data, features, target):
    model_data = prepare_data_2(data, features, target)
    # train, test
    train, test = model_data.randomSplit([0.8, 0.2], seed=12345)
    # model
    lr = LinearRegression(featuresCol='features', labelCol='label', maxIter=10, regParam=0.01)
    # train model
    model = lr.fit(train)
    # check the model on the test data
    prediction = model.transform(test)
    prediction.show(15)
    evaluation_result = evaluator.evaluate(prediction)
    print("Evaluation result: {}".format(evaluation_result))
    return model


def prepare_and_train_3(data, features, target):
    model_data = prepare_data_2(data, features, target)
    # train, test
    train, test = model_data.randomSplit([0.6, 0.4], seed=12345)
    # model
    lr = LinearRegression(featuresCol='features', labelCol='label', maxIter=20, regParam=0.01)
    paramGrid = ParamGridBuilder() \
        .addGrid(lr.regParam, [0.1, 0.01]) \
        .addGrid(lr.fitIntercept, [False, True]) \
        .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0]) \
        .build()
    tvs = TrainValidationSplit(estimator=lr,
                               estimatorParamMaps=paramGrid,
                               evaluator=evaluator,
                               trainRatio=0.8)
    # train model
    model = tvs.fit(train)
    # check the model on the test data
    prediction = model.transform(test)
    prediction.show(15)
    evaluation_result = evaluator.evaluate(prediction)
    print("Evaluation result: {}".format(evaluation_result))
    return model


# MODEL 1 #
print("=== MODEL 1 ===")
features = ["area,latitude,longitude,Bedrooms,Bathrooms,Balcony,Lift"]


model_1 = prepare_and_train_1(data, features, target)
model_1.write().overwrite().save(model_dir + "/model_1")


# MODEL 2 #
print("=== MODEL 2 ===")

features = ["area,latitude,longitude,Bedrooms,Bathrooms,Balcony,Lift"]

model_2 = prepare_and_train_2(data, features, target)
model_2.write().overwrite().save(model_dir + "/model_2")


# MODEL 3 #
print("=== MODEL 3 ===")
features = ["area,latitude,longitude,Bedrooms,Bathrooms,Balcony,Lift"]

model_3 = prepare_and_train_3(data, features, target)
model_3.write().overwrite().save(model_dir + "/model_3")


# MODEL 4 #
print("=== MODEL 4 ===")
features = ["area,latitude,longitude,Bedrooms,Bathrooms,Balcony,Lift"]

model_4 = prepare_and_train_2(data, features, target)
model_4.write().overwrite().save(model_dir + "/model_4")


# MODEL 5 #
print("=== MODEL 5 ===")
features = ["area,latitude,longitude,Bedrooms,Bathrooms,Balcony,Lift"]

model_5 = prepare_and_train_3(data, features, target)
model_5.write().overwrite().save(model_dir + "/model_5")
