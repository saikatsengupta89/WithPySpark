from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def classifyGenderFlag(gender):
  s=str(gender).lower()
  print(s)
  female_list=["cis female", "f", "female", "woman", "femake", "female ", "cis-female/femme",
               "female (cis)", "femail"]
  male_list  =["male", "m", "male-ish", "maile", "mal", "male (cis)", "make", "male ", "man",
               "msle","mail", "malr", "cis man", "cis male"]
  if (s in female_list):
    return "Female"
  elif (s in male_list):
    return "Male"
  else:
    return "Transgender"

#SYNTAX: lambda <args> : <return Value> if <condition > ( <return value > if <condition> else <return value>)
#lambda doesn't support if elif else but only if-else
genderFlag= lambda x: 0 if str(x).lower() in ["cis female", "f", "female", "woman", "femake", "female ", \
                                             "cis-female/femme","female (cis)", "femail"] \
                        else (1 if str(x).lower() in ["male", "m", "male-ish", "maile", "mal",
                                                      "male (cis)", "make", "male ", "man", "msle",\
                                                      "mail", "malr", "cis man", "cis male"]\
                                else -1)

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("create my custom UDF") \
    .config("spark.executor.memory", "2g") \
    .getOrCreate()

sampleList = [
    {"PID": 28199211, "Name": "Ryan", "Gender": 'M'},
    {"PID": 28193211, "Name": "Sunil", "Gender": 'Male'},
    {"PID": 23199211, "Name": "Geeta", "Gender": 'f'},
    {"PID": 23499211, "Name": "June", "Gender": 'Female'},
    {"PID": 34199211, "Name": "Kalpz", "Gender": 'NA'}
]
# define schema
sample_schema = StructType([
    StructField("PID", IntegerType()),
    StructField("Name", StringType()),
    StructField("Gender", StringType())
])
# parsing the JSON
sampleRDD = spark.sparkContext.parallelize(sampleList)
sampleDF = spark.createDataFrame(sampleList, sample_schema)
sampleDF.show()
sampleDF.createOrReplaceTempView("sample_data")

# register the UDF with the current spark session
genderClass = lambda s: classifyGenderFlag(s)
spark.udf.register("getGenderClass", genderClass)
spark.udf.register("getGenderFlag", genderFlag)

transformedDF = spark.sql(
    "select " +
    "PID, " +
    "name, " +
    "getGenderClass(gender) as gender, " +
    "getGenderFlag(gender) as gender_flag " +
    "from sample_data"
)
transformedDF.show()