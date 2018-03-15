import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, lit

WAREHOUSE_DIR = "C:\\Data\\spark-warehouse"
TABLE_NAME = "Events"

def usage():
    print(sys.argv[0], "<inputURI>")

if __name__ == "__main__":
    if len(sys.argv) > 2:
        # TODO USAGE
        usage()
        sys.exit(1)
    inputURI = sys.argv[1]

    sparkSession = SparkSession.builder\
        .master("local[2]")\
        .appName("Agenda")\
        .enableHiveSupport() \
        .config("spark.sql.warehouse.dir", WAREHOUSE_DIR) \
        .config("spark.sql.parquet.compression.codec","gzip") \
        .getOrCreate()

    df = sparkSession.read.json(inputURI)
    df.printSchema()

    fieldsToFilter = (
        "records.city",
        "records.date_start",
        "records.date_end",
        "records.pricing_info",
        "records.address",
        "records.department",
        "records.title"
    )

    partitioningTuple = (
        "department",
        "city"
    )

    tableOptions = {
        'dir': WAREHOUSE_DIR,
        'tableName': "Events"
    }

    df.select(explode(df.records.fields).alias("records")) \
        .select(*fieldsToFilter) \
        .write.mode("overwrite") \
        .partitionBy(*partitioningTuple) \
        .format("parquet") \
        .save("{dir}/{tableName}/key=1".format(**tableOptions))
    #sparkSession.read.parquet("spark-warehouse/parking_velo/department=Bouches-du-Rhône").show()

    sparkSession.read \
        .parquet("{dir}/{tableName}/key=1/department=Bouches-du-Rhône".format(**tableOptions)) \
        .withColumn("soldOut", lit(False)) \
        .write.mode("overwrite").partitionBy("city").format("parquet") \
        .save("{dir}/{tableName}/key=2/department=Bouches-du-Rhône".format(**tableOptions))

    mergedDataframe = sparkSession.read \
        .option("mergeSchema","true") \
        .parquet("{dir}/{tableName}".format(**tableOptions))

    mergedDataframe.printSchema()
    mergedDataframe.show()
