#****************************************************************************
# (C) Cloudera, Inc. 2020-2023
#  All rights reserved.
#
#  Applicable Open Source License: GNU Affero General Public License v3.0
#
#  NOTE: Cloudera open source products are modular software products
#  made up of hundreds of individual components, each of which was
#  individually copyrighted.  Each Cloudera open source product is a
#  collective work under U.S. Copyright Law. Your license to use the
#  collective work is as provided in your written agreement with
#  Cloudera.  Used apart from the collective work, this file is
#  licensed for your use pursuant to the open source license
#  identified above.
#
#  This code is provided to you pursuant a written agreement with
#  (i) Cloudera, Inc. or (ii) a third-party authorized to distribute
#  this code. If you do not have a written agreement with Cloudera nor
#  with an authorized and properly licensed third party, you do not
#  have any rights to access nor to use this code.
#
#  Absent a written agreement with Cloudera, Inc. (“Cloudera”) to the
#  contrary, A) CLOUDERA PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY
#  KIND; (B) CLOUDERA DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED
#  WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT LIMITED TO
#  IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND
#  FITNESS FOR A PARTICULAR PURPOSE; (C) CLOUDERA IS NOT LIABLE TO YOU,
#  AND WILL NOT DEFEND, INDEMNIFY, NOR HOLD YOU HARMLESS FOR ANY CLAIMS
#  ARISING FROM OR RELATED TO THE CODE; AND (D)WITH RESPECT TO YOUR EXERCISE
#  OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, CLOUDERA IS NOT LIABLE FOR ANY
#  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR
#  CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO, DAMAGES
#  RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF
#  BUSINESS ADVANTAGE OR UNAVAILABILITY, OR LOSS OR CORRUPTION OF
#  DATA.
#
# #  Author(s): Paul de Fusco
#***************************************************************************/


import os
import numpy as np
import pandas as pd
from datetime import datetime
import dbldatagen as dg
import dbldatagen.distributions as dist
from dbldatagen import DataGenerator
from pyspark.sql.types import LongType, FloatType, IntegerType, StringType, \
                              DoubleType, BooleanType, ShortType, \
                              TimestampType, DateType, DecimalType, \
                              ByteType, BinaryType, ArrayType, MapType, \
                              StructType, StructField

class DataGen:

    '''Class to Generate Banking Data'''

    def __init__(self, spark):
        self.spark = spark

    def dataGen(self, shuffle_partitions_requested = 10, partitions_requested = 10, data_rows = 10000):

        # partition parameters etc.
        self.spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)

        dataSpec = (DataGenerator(self.spark, rows=data_rows, partitions=partitions_requested)
                    .withColumn("transaction_type", "string", values=["purchase", "cash_advance"], random=True, weights=[9, 1])
                    .withColumn("event_ts", "timestamp", begin="2023-01-01 01:00:00",end="2023-12-31 01:00:00",interval="1 minute", random=True)
                    .withColumn("longitude", "float", minValue=-125, maxValue=-66.9345, random=True)
                    .withColumn("latitude", "float", minValue=24.3963, maxValue=49.3843, random=True)
                    .withColumn("transaction_currency", values=["USD", "EUR", "KWD", "BHD", "GBP", "CHF", "MEX"])
                    .withColumn("transaction_amount", "decimal", minValue=0.01, maxValue=30000, random=True)
                    .withColumn("floatcol1", "float", minValue=40, maxValue=50, random=True, distribution="normal")
                    .withColumn("floatcol2", "float", minValue=40, maxValue=50, random=True, distribution="normal")
                    )

        df = dataSpec.build()

        return df

spark = SparkSession \
    .builder \
    .appName("DATA GENERATION") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")\
    .config("spark.sql.catalog.spark_catalog.type", "hive")\
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")\
    .getOrCreate()

DG = DataGen(spark)

STORAGE="s3a://paul-aug26-buk-a3c2b50a/data/"
sparkDf = DG.dataGen()
sparkDf.write.format("parquet").mode("overwrite").save(STORAGE+"pdefusco/modin/tsdata")
#transactionsDf.write.format("json").mode("overwrite").save("/home/cdsw/jsonData2.json")
