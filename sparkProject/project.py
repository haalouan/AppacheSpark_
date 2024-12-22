import math
import pyspark
import numpy as np
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.ml.tuning import TrainValidationSplit, ParamGridBuilder
import os

spark = SparkSession.builder.master("local[*]").appName("Spark").getOrCreate()

ddlSchema = '''
    id int,
    course_id int,
    rate float,
    date string,
    display_name string,
    comment string
'''

dataFrame = spark.read.format('csv').schema(ddlSchema).option('header', True).load('new_file.csv') 

dataFrame = dataFrame.select('id', 'course_id', 'rate')

dataFrame = dataFrame.dropna(subset=['id', 'course_id', 'rate'])

#########
# Train-test split
trainSet, testSet = dataFrame.randomSplit([0.8, 0.2], seed=42)

# Ensure Overlap Between Train and Test Sets
train_users = trainSet.select("id").distinct()
train_items = trainSet.select("course_id").distinct()

testSet = testSet.join(train_users, on="id", how="inner")
testSet = testSet.join(train_items, on="course_id", how="inner")

# Check Overlap
if testSet.count() == 0:
    # print("Warning: Insufficient overlap. Using full dataset for training and sampling for evaluation.")
    trainSet = dataFrame
    testSet = dataFrame.sample(False, 0.2, seed=42)

# Build ALS Model
als = ALS(
    userCol="id",
    itemCol="course_id",
    ratingCol="rate",
    coldStartStrategy="drop",
    nonnegative=True
)

# Train ALS Model
model = als.fit(trainSet)

# Make Predictions
predictions = model.transform(testSet)

# Evaluate Model
evaluator = RegressionEvaluator(
    metricName="rmse",
    labelCol="rate",
    predictionCol="prediction"
)

rmse = evaluator.evaluate(predictions)

user_id = 58809400

course_ids = dataFrame.select("course_id").distinct()

user_data = course_ids.withColumn("id", lit(user_id))


user_predictions = model.transform(user_data)

rated_courses = dataFrame.filter(col("id") == user_id).select("course_id").distinct()
unrated_courses = user_predictions.join(rated_courses, "course_id", "left_anti")

recommended_courses = unrated_courses.orderBy(col("prediction").desc())

recommended_courses = recommended_courses.limit(5)

####
course_ids_to_check = [row['course_id'] for row in recommended_courses.select('course_id').collect()]

data2 = spark.read.format('csv').option('header', True).option('inferSchema', True).load("Course_info.csv")
data2 = data2.select(col('id'), col('title'), col('is_paid'), col('price'))

schema = StructType([
    StructField("id", DoubleType(), True),
    StructField("title", StringType(), True),
    StructField("is_paid", StringType(), True),
    StructField("price", StringType(), True)
])

course_info = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)

for n in course_ids_to_check:
    course_info2 = data2.filter(data2.id == n)
    course_info = course_info.union(course_info2)

os.system("clear")
recommended_courses = recommended_courses.join(course_info, recommended_courses.course_id == course_info.id, how='inner')
print(f"Top 5 recommended courses for user {user_id}:")
if recommended_courses.isEmpty():
    os.system("clear")
    print(f"User with ID {user_id} does not exist.")
    exit()
recommended_courses = recommended_courses.drop('id')
recommended_courses.show()