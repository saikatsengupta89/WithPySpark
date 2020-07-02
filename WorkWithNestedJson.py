from pyspark.sql import SparkSession
from pyspark.sql import functions as f
spark= SparkSession.builder\
                   .master("local[*]")\
                   .appName("Normalizing Nested JSON")\
                   .config("spark.executor.memory", "2g")\
                   .getOrCreate()
json_data= spark.read.option("multiline","true").json("C:/PycharmProjects/WorkWithSpark/SampleJsonData.json")
json_data.printSchema()

# first explode the persons dataset where multiple person are coming into a single structure as array
persons_t1= json_data.select(f.explode("persons").alias("persons"))
#persons_t1.printSchema()

# second select the person elements dataset from the available JSON
persons_t2= persons_t1.select(f.col("persons.name").alias("name"),
                              f.col("persons.age").alias("age"),
                              f.col("persons.address").alias("address"),
                              f.col("persons.cars").alias("cars"))
#persons_t2.printSchema()
#persons_t2.show()

# third select directly available rows and explode the required field
persons_t3= persons_t2.select(f.col("name"),
                              f.col("age"),
                              f.col("address.country").alias("country"),
                              f.col("address.state").alias("state"),
                              f.col("address.city").alias("city"),
                              f.col("cars").alias("cars"))
#persons_t3.printSchema()

# second select the first set of directly available rows and explode the required field
persons_t4= persons_t3.select(f.col("name"),
                              f.col("age"),
                              f.col("country"),
                              f.col("state"),
                              f.col("city"),
                              f.explode(f.col("cars")).alias("cars"))
#persons_t4.printSchema()

persons_t5= persons_t4.select(f.col("name"),
                              f.col("age"),
                              f.col("country"),
                              f.col("state"),
                              f.col("city"),
                              f.col("cars.name").alias("car_name"),
                              f.col("cars.models").alias("car_models"))

persons_t6= persons_t5.select(f.col("name"),
                              f.col("age"),
                              f.col("country"),
                              f.col("state"),
                              f.col("city"),
                              f.col("car_name"),
                              f.explode(f.col("car_models")).alias("car_model"))
persons_t6.show()