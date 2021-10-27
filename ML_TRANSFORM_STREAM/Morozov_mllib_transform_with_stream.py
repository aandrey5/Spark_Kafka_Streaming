#/spark2.4/bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5,com.datastax.spark:spark-cassandra-connector_2.11:2.4.2 --driver-memory 512m --driver-cores 1 --master local[1]

import json

from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.regression import LinearRegressionModel
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType


spark = SparkSession.builder.appName("mllib_predict_app").getOrCreate()

checkpoint_location = "tmp/ml_checkpoint"


# upload the best model
data_path = "ml_data/data_csv/"
model_dir = "ml_data/models"
model = LinearRegressionModel.load(model_dir + "/model_5/bestModel/data/")

# read stream
schema = StructType.fromJson(json.loads('{"fields":[{"metadata":{},"name":"price","nullable":true,"type":"integer"},{"metadata":{},"name":"Address","nullable":true,"type":"string"},{"metadata":{},"name":"area","nullable":true,"type":"integer"},{"metadata":{},"name":"latitude","nullable":true,"type":"integer"},{"metadata":{},"name":"longitude","nullable":true,"type":"integer"},{"metadata":{},"name":"Bedrooms","nullable":true,"type":"integer"},{"metadata":{},"name":"Bathrooms","nullable":true,"type":"integer"},{"metadata":{},"name":"Balcony","nullable":true,"type":"integer"},{"metadata":{},"name":"Status","nullable":true,"type":"string"},{"metadata":{},"name":"neworold","nullable":true,"type":"string"},{"metadata":{},"name":"parking","nullable":true,"type":"string"},{"metadata":{},"name":"Furnished_status","nullable":true,"type":"string"},{"metadata":{},"name":"Lift","nullable":true,"type":"integer"},{"metadata":{},"name":"Landmarks","nullable":true,"type":"string"},{"metadata":{},"name":"type_of_building","nullable":true,"type":"string"},{"metadata":{},"name":"desc","nullable":true,"type":"string"},{"metadata":{},"name":"Price_sqft","nullable":true,"type":"string"}],"type":"struct"}'))

features = ["latitude,longitude,Bedrooms,Bathrooms,Balcony,Lift"]

data = spark \
    .readStream \
    .format("csv") \
    .schema(schema) \
    .options(header=True, maxFilesPerTrigge=1) \
    .load(data_path)

# Transform data

data = data.withColumn("area", data["area"].cast(IntegerType()))
data = data.withColumn("latitude", data["latitude"].cast(IntegerType()))
data = data.withColumn("longitude", data["longitude"].cast(IntegerType()))
data = data.withColumn("Bedrooms", data["Bedrooms"].cast(IntegerType()))
data = data.withColumn("Bathrooms", data["Bathrooms"].cast(IntegerType()))
data = data.withColumn("Balcony", data["Balcony"].cast(IntegerType()))
data = data.withColumn("Lift", data["Lift"].cast(IntegerType()))
data = data.withColumn("price", data["price"].cast(IntegerType()))

data = data.fillna(0)


def prepare_data(df, features):
    # features
    f_columns = ",".join(features).split(",")
    # model data set
    model_data = df.select(f_columns)
    # prepare categorical columns
    text_columns = ['Balcony', 'Lift']
    output_text_columns = [c + "_index" for c in text_columns]
    for c in text_columns:
        string_indexer = StringIndexer(inputCol=c, outputCol=c + '_index').setHandleInvalid("keep")
        model_data = string_indexer.fit(model_data).transform(model_data)
    # update f_columns with indexed text columns
    f_columns = list(filter(lambda c: c not in text_columns, f_columns))
    f_columns += output_text_columns
    # preparation
    assembler = VectorAssembler(inputCols=f_columns, outputCol='features')
    model_data = assembler.transform(model_data)
    return model_data.select('features')


def process_batch(df, epoch):
    model_data = prepare_data(df, features)
    prediction = model.transform(model_data)
    prediction.show()


def foreach_batch_output(df):
    from datetime import datetime as dt
    date = dt.now().strftime("%Y%m%d%H%M%S")
    return df\
        .writeStream \
        .trigger(processingTime='%s seconds' % 10) \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", checkpoint_location + "/" + date)\
        .start()

stream = foreach_batch_output(data)

stream.awaitTermination()
