from pyspark.sql import SparkSession
from pyspark.sql import types as types

"""
Input:
+-----+----+------+
|class|male|female|
+-----+----+------+
|    1|   2|     1|
|    2|   0|     2|
|    3|   2|     0|
+-----+----+------+

Output:
+-----+------+
|class|gender|
+-----+------+
|    1|     m|
|    1|     m|
|    1|     f|
|    2|     f|
|    2|     f|
|    3|     m|
|    3|     m|
+-----+------+

"""

def getMale(cls, male_cnt):
    burst=list()
    for i in range(male_cnt):
        burst.append((cls, 'm'))
    return burst

def getFemale(cls, female_cnt):
    burst=list()
    for i in range(female_cnt):
        burst.append((cls, 'f'))
    return burst

spark = SparkSession.builder\
                    .master("local[*]")\
                    .appName("transformHeadCountToGender")\
                    .getOrCreate()

data_class = spark.read.option("header","true")\
                       .csv("C:/PycharmProjects/WithPySpark/classData.csv")
data_class.show()
transform_list =list()
clean_list= list()
for row in data_class.rdd.collect():
    if (row['male'] !=0):
        transform_list.append(getMale(row['class'], int(row['male'])))
    if (row['female'] !=0):
        transform_list.append(getFemale(row['class'], int(row['female'])))

for element in transform_list:
     for tup in element:
         clean_list.append(tup)

cls_schema = types.StructType([
  types.StructField("class", types.StringType()),
  types.StructField("gender", types.StringType())
])
transformed_data = spark.createDataFrame(clean_list, schema=cls_schema)
transformed_data.show()
transformed_data.registerTempTable("cls_data")

dataset_revert= spark.sql ("select "+
                           "class, "+
                           "sum(case when gender='m' then 1 else 0 end) male_cnt, "+
                           "sum(case when gender='f' then 1 else 0 end) female_cnt "+
                           "from cls_data "+
                           "group by class "+
                           "order by 1")
dataset_revert.show()