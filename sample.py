import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
# Example: Use map to combine fields in all records
# of a DynamicFrame
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from awsglue.context import GlueContext

# Create GlueContext
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)

# Create a DynamicFrame and view its schema
# medicare = glueContext.create_dynamic_frame.from_options(
#         "s3",
#         {"paths": ["s3://awsglue-datasets/examples/medicare/Medicare_Hospital_Provider.csv"]},
#         "csv",
#         {"withHeader": True})
# print("Schema for medicare DynamicFrame:")
# medicare.printSchema()

spark = SparkSession \
    .builder \
    .appName("how to read csv file") \
    .getOrCreate()
df = spark.read.csv("test.csv",header=True,inferSchema=True)

dyf = DynamicFrame.fromDF(df, glueContext, "test_nest")
#dyf.show()

def add_astrisk(rec): 
  #print(rec)
  m_id = str(rec["id"])
  print("".join([m_id[:-2],"*","01"]))
  rec["id"] = "".join([m_id[:-2],"*","01"])
  print(rec)
  return rec


dyf = dyf.map(f = add_astrisk)
print("Schema for mapped_medicare DynamicFrame:")
dyf.printSchema()
dyf.show()




















# from awsglue.context import GlueContext
# from awsglue.job import Job
# from awsglue.utils import getResolvedOptions

# sc = SparkContext.getOrCreate()
#rdd = sc.parallelize([1,2,3,4])
#print(type(rdd))
# spark = SparkSession \
#     .builder \
#     .appName("how to read csv file") \
#     .getOrCreate()
# df = spark.read.csv("household_power_consumption.csv",header=True,inferSchema=True)
# df.printSchema()
# print(df.show(5))
# df.registerTempTable("df")
# df  = spark.sql("select Time from df").show(2)


# print("Hello sir")
# class GluePythonSampleTest:
#     def __init__(self):
#         params = []
#         if '--JOB_NAME' in sys.argv:
#             params.append('JOB_NAME')
#         args = getResolvedOptions(sys.argv, params)

#         self.context = GlueContext(SparkContext.getOrCreate())
#         self.job = Job(self.context)

#         if 'JOB_NAME' in args:
#             jobname = args['JOB_NAME']
#         else:
#             jobname = "test"
#         self.job.init(jobname, args)

#     def run(self):
#         dyf = read_json(self.context, "s3://awsglue-datasets/examples/us-legislators/all/persons.json")
#         dyf.printSchema()

#         self.job.commit()


# def read_json(glue_context, path):
#     dynamicframe = glue_context.create_dynamic_frame.from_options(
#         connection_type='s3',
#         connection_options={
#             'paths': [path],
#             'recurse': True
#         },
#         format='json'
#     )
#     return dynamicframe






# if __name__ == '__main__':
#     GluePythonSampleTest().run()
