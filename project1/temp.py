from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import monotonically_increasing_id



if __name__ == '__main__':
    from pyspark.sql.functions import explode
    from pyspark.sql.window import Window

    spark = SparkSession.builder.appName('oracle_connection') \
        .config('spark.jars.packages',
                'com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.26.0,com.google.cloud.bigdataoss:gcs-connector:hadoop3-1.9.5,com.google.guava:guava:r05') \
        .config('spark.jars', r"C:\spark\jars\ojdbc8.jar") \
        .config("spark.memory.offHeap.enabled", "true")\
        .config("spark.memory.offHeap.size", "10g")\
        .getOrCreate()
    spark.conf.set('temporaryGcsBucket', 'temp_gcs_bucket1')
    spark._jsc.hadoopConfiguration() \
        .set('google.cloud.auth.service.account.json.keyfile',
             r"C:\Users\Administrator\Downloads\innate-temple-351706-48d7a10baa48.json")

    spark._jsc.hadoopConfiguration().set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
    # This is required if you are using service account and set true,
    spark._jsc.hadoopConfiguration().set('fs.gs.auth.service.account.enable', 'true')


    driver = 'oracle.jdbc.driver.OracleDriver'
    url = 'jdbc:oracle:thin:@localhost:1521/xe'
    # host = 'localhost/xe'
    user = 'sys AS SYSDBA'
    password = 'sys'
    # server = 'oracleserver'
    # service_name = 'XE'
    table1 = 'SOURCE_TABLE1'
    # query = 'select * from SOURCE_TABLE1'
    # query1 = 'select count(*) from HR.SOURCE_TABLE'

    # table2 = 'NJ_72_A'
    # table3 = 'NJ_72_B'
    # table4 = 'NJ_70'

    source_table = spark.read.format('jdbc')\
                    .option('driver',driver)\
                    .option('url',url)\
                    .option('user',user)\
                    .option('dbtable',table1)\
                    .option('password',password)\
                    .load()
    # source_table.show()
    # print(source_table.count())


    NJ_85 = source_table.filter(col('table1') == '85.(LC)').filter(col('state') == 'NJ')\
                .withColumnRenamed("KEY1", "class_code").withColumnRenamed("KEY2", "Coverage") \
                .withColumnRenamed("KEY3", "Symbol").withColumnRenamed("KEY4", "Construction code") \
                .withColumn('symbol', explode_outer(split("symbol", "&")))\
                .withColumn('construction code',explode_outer(split("construction code", "or"))) \
                .withColumn("class_code", lpad("class_code", 4, '0')).withColumn("Id", 1001 + monotonically_increasing_id())\
                .select("Id","COUNTRY","STATE","TABLE1","STATE1","EFFECTIVE","EXPIRATION","class_code","Coverage","symbol","Construction code","FACTOR")

    # NJ_85.write.format('bigquery') \
    #     .option('table', '{}:{}.{}'.format('innate-temple-351706','as_dataflow_practice','NJ_table_85')) \
    #     .mode('overwrite').save()

    (NJ_85.write.format("bigquery")
     .option("table", "as_dataflow_practice.NJ_table_85").mode("append")
     .save())
    # NJ_85.show()
    # print(NJ_85.count())

