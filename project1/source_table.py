from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import monotonically_increasing_id



if __name__ == '__main__':
    spark = SparkSession.builder.appName('oracle_connection')\
                    .config('spark_jars',r"C:\spark\jars\ojdbc8.jar").getOrCreate()

    driver = 'oracle.jdbc.driver.OracleDriver'
    url = 'jdbc:oracle:thin:@localhost:1521/xe'
    host = 'localhost'
    user = 'sys AS SYSDBA'
    password = 'sys'
    server = 'oracleserver'
    service_name = 'xe'
    table1 = 'SOURCE_TABLE1'
    query = 'select * from SOURCE_TABLE1'
    # query1 = 'select count(*) from HR.SOURCE_TABLE'

    # table2 = 'NJ_72_A'
    # table3 = 'NJ_72_B'
    # table4 = 'NJ_70'

    source_table = spark.read.format('jdbc')\
                    .option('driver',driver)\
                    .option('url',url)\
                    .option('host',host)\
                    .option('user',user)\
                    .option('komal',table1)\
                    .option('password',password)\
                    .option('query',query) \
                    .load()
    # source_table.show()
    print(source_table.count())

    w = Window().orderBy("state")

    NJ_85 = source_table.filter(col('table1') == '85.(LC)').filter(col('state') == 'NJ')\
                .withColumnRenamed("KEY1", "class_code").withColumnRenamed("KEY2", "Coverage") \
                .withColumnRenamed("KEY3", "Symbol").withColumnRenamed("KEY4", "Construction code") \
                .withColumn('symbol', explode_outer(split("symbol", "&")))\
                .withColumn('construction code',explode_outer(split("construction code", "or"))) \
                .withColumn("class_code", lpad("class_code", 4, '0')).withColumn("Id",1000+row_number().over(w))\
                .select("Id","COUNTRY","STATE","TABLE1","STATE1","EFFECTIVE","EXPIRATION","class_code","Coverage","symbol","Construction code","FACTOR")


    NJ_85.show()
    print(NJ_85.count())


    NJ_85.write.csv(r'D:\GCP\project_gcp\output_table\NJ_85.csv',sep=",")

#------------------------------------------------------------------------------------------------------------------------------------------#

    #Table2#

    NJ_72_1 = source_table.filter(col('table1') == '72.E.2.b.(1)(LC)').filter(col('state') == 'NJ') \
        .withColumnRenamed("KEY1", "class_code").withColumnRenamed("KEY2", "Coverage") \
        .withColumnRenamed("KEY3", "Symbol").withColumnRenamed("KEY4", "Construction code") \
        .withColumn('symbol', explode_outer(split("symbol", "&")))\
        .withColumn('construction code', explode_outer(split("construction code", "or")) )\
        .withColumn("Id", 1001 + monotonically_increasing_id()) \
        .select("Id", "COUNTRY", "STATE", "TABLE1", "STATE1", "EFFECTIVE", "EXPIRATION", "class_code", "Coverage",
                "symbol", "Construction code", "FACTOR")

    # NJ_72_1.show()
    # print(NJ_72_1.count())

#----------------------------------------------------------------------------------------------------------------------------------------#

    #Table3#

    NJ_72_2 = source_table.filter(col('table1') == '72.E.2.c.(2)(LC)').filter(col('state') == 'NJ') \
        .withColumnRenamed("KEY1", "class_code").withColumnRenamed("KEY2", "Coverage") \
        .withColumnRenamed("KEY3", "Symbol").withColumnRenamed("KEY4", "Construction code") \
        .withColumn('symbol', explode_outer(split("symbol", "&"))) \
        .withColumn('construction code', explode_outer(split("construction code", "or"))) \
        .withColumn("Id", 1000 + row_number().over(w)) \
        .select("Id", "COUNTRY", "STATE", "TABLE1", "STATE1", "EFFECTIVE", "EXPIRATION", "class_code", "Coverage",
                "symbol", "Construction code", "FACTOR")

    # NJ_72_2.show()
    # print(NJ_72_2.count())

#-----------------------------------------------------------------------------------------------------------------------------------------#

    #table4#

    NJ_70 = source_table.filter(col('table1') == '70.E.2.e.BGII(LC)').filter(col('state') == 'NJ') \
        .withColumnRenamed("KEY1", "class_code").withColumnRenamed("KEY2", "Coverage") \
        .withColumnRenamed("KEY3", "Symbol").withColumnRenamed("KEY4", "Construction code") \
        .withColumn('symbol', explode_outer(split("symbol", "&"))) \
        .withColumn('construction code', explode_outer(split("construction code", "or"))) \
        .withColumn("Id", 1001 + monotonically_increasing_id())  \
        .select("Id", "COUNTRY", "STATE", "TABLE1", "STATE1", "EFFECTIVE", "EXPIRATION", "class_code", "Coverage",
                "symbol", "Construction code", "FACTOR")
    #
    # NJ_70.show()
    # print(NJ_70.count())

#--------------------------------------------------------------------------------------------------------------------------------------#

    #tablw5#

    NJ_85_TerrMult = source_table.filter(col('table1') == '85.TerrMult(LC)').filter(col('state') == 'NJ') \
        .withColumnRenamed("KEY1", "class_code").withColumnRenamed("KEY2", "Coverage") \
        .withColumnRenamed("KEY3", "Symbol").withColumnRenamed("KEY4", "Construction code") \
        .withColumn('symbol', explode_outer(split("symbol", "&"))) \
        .withColumn('construction code', explode_outer(split("construction code", "or"))) \
        .withColumn("Id", 1000 + row_number().over(w)) \
        .select("Id", "COUNTRY", "STATE", "TABLE1", "STATE1", "EFFECTIVE", "EXPIRATION", "class_code", "Coverage",
                "symbol", "Construction code", "FACTOR")

    # NJ_85_TerrMult.show()
    # print(NJ_85_TerrMult.count())







