from pyspark.sql import SparkSession
from pyspark import SparkContext,SQLContext,SparkConf,StorageLevel
from pyspark.sql.types import StructType,StructField, StringType, IntegerType 
from pyspark.sql.types import ArrayType, DoubleType, BooleanType, DateType
# from pyspark.sql.functions import array_contains
import pyarrow as pa
#import numpy as np
import pandas as pd

# from pyspark.sql import SparkSession
# from pyspark import SparkContext,SparkConf
# spark_session = SparkSession.builder.master("spark://127.0.0.1:7077").getOrCreate()
# session = SparkContext(SparkConf().setMaster("spark://127.0.0.1:7077"))

# spark = sc.getOrCreate()

spark = SparkSession.builder.appName("ReadParquertData").getOrCreate()
data_frame = spark.read.parquet("D:\\Learning\\BigData\\Datasets\\ParquetData\\accdata.parquet")

data_frame.printSchema()
data_frame.select("fulldate","pltitle","credit", "debit").show(100)
data_frame.select("SLtitle").distinct().count()
data_frame.filter(data_frame['Debit'] > 1000000).orderBy(data_frame["Debit"].desc()).select("fulldate","SLtitle","Debit").limit(20).show()
sales_df = spark.read.parquet("D:\\Learning\\BigData\\Datasets\\ParquetData\\salesdata.parquet")
sales_df.count()
sales_df.printSchema()
sales_df.createOrReplaceTempView("salesData")
spark.sql("select distinct salesdocumenttitle from salesData ").show(20)
spark.sql("select monthname,sum(salesQuantity) salesQty, sum(salesPrice) salesPrice from salesdata  where salesdocumenttitle ='خالص فروش' group by monthname" ).show(20)
spark.sql("select sum(salesQuantity) salesQty, sum(salesPrice) salesPrice from salesdata  where salesdocumenttitle ='خالص فروش'" ).show(20)
qry = "select CustomerName, ProductTitle, sum(salesQuantity) salesQty, sum(salesPrice) salesPrice from salesdata "
qry += " where salesdocumenttitle ='خالص فروش' "
# using grouping Sets
#qry += " group by Grouping SETS ((CustomerName,ProductTitle),(CustomerName),(ProductTitle),()) order by 3"
# using equivalent cube
#qry += " group by CustomerName,ProductTitle WITH CUBE order by 3 desc"
# using rollup equivalent to GROUPING SETS ((CustomerName,ProductTitle),(CustomerName),())
qry += " group by CustomerName,ProductTitle WITH ROLLUP order by 3 desc"
spark.sql(qry).show(100)
# read accouting data 
df = spark.read.parquet("D:\\Learning\\BigData\\Datasets\\ParquetData\\FIN3_Voucher.parquet")
df.createOrReplaceTempView("fin3_voucher")
df = spark.read.parquet("D:\\Learning\\BigData\\Datasets\\ParquetData\\FIN3_VoucherItem.parquet")
df.createOrReplaceTempView("fin3_voucherItem")
qry = "SELECT * FROM fin3_voucher VH JOIN fin3_voucherItem VI ON vi.VoucherRef = v.VoucherID"
qry = "select * FROM salesData"
spark.sql(qry).show(20)
dl_df = spark.read.parquet("D:\\Learning\\BigData\\Datasets\\ParquetData\\fin3_dl.parquet")
dl_df.printSchema()
dl_df.createOrReplaceTempView("fin3_dl")