import os
from datetime import date, datetime
from time import time
from subprocess import call
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.types import FloatType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import DateType


HDFSPATH = "hdfs://193.174.205.250:54310/"
CDCPATH = HDFSPATH + "home/wolfedgar/"


# Fürs Löschen, wenn nötig.
def delete_from_hdfs(path):
    call("hdfs dfs -rm -R " + path,
         shell=True)

delete_from_hdfs(CDCPATH + "cdcstations.parquet")
delete_from_hdfs(CDCPATH + "product.parquet")

# Read parquets
def read_cdc_from_parquet(spark):
    dfcdcStations = spark.read.parquet(CDCPATH + "cdcstations.parquet")
    dfcdcStations.createOrReplaceTempView("cdcstations")
    dfcdcStations.cache()
    
    dfProductData = spark.read.parquet(CDCPATH + "cdcdata.parquet")
    dfProductData.createOrReplaceTempView("cdcdata")
    dfProductData.cache()
    
read_cdc_from_parquet(spark)

# 1.)

def import_cdc_data(scon, spark, path):
    stationLines = scon.textFile(path + "TU_Stundenwerte_Beschreibung_Stationen.txt")
    stationLinesFiltered = stationLines.zipWithIndex().filter(lambda x: x[1] > 1).map(lambda x: x[0])
    stationSplitLines = stationLinesFiltered.map(
        lambda l: (
            int(l[0:5].strip()),
            datetime.strptime(l[6:14], "%Y%m%d"),
            datetime.strptime(l[15:23], "%Y%m%d"),
            int(l[34:38].strip()),
            float(l[43:50]),
            float(l[53:60].strip()),
            l[61:101],
            l[102:125]
            ))

    stationschema = StructType([
            StructField('id', IntegerType(), True),
            StructField('von_datum', DateType(), True),
            StructField('zu_datum',   DateType(), True),
            StructField('hoehe',    IntegerType(),  True),
            StructField('breite',   FloatType(),  True),
            StructField('laenge',   FloatType(),  True),
            StructField('name', StringType(), True),
            StructField('bundesland', StringType(), True)
        ])
    
    stationDataFrame = spark.createDataFrame(stationSplitLines, schema=stationschema)
    
    #stationDataFrame.select("id", "höhe", "breite", "name", "bundesland").show(truncate=False)
    
    stationDataFrame.createOrReplaceTempView("cdcstations")

    stationDataFrame.write.mode('overwrite').parquet(CDCPATH + "cdcstations.parquet")
    
    stationDataFrame.cache()
    print("Imported cdcstations")
    return stationDataFrame


    
import_cdc_data(scon, spark, "/data/cdc/hourly/")



spark.sql("SELECT * FROM cdcstations WHERE trim(bundesland) = 'Bayern'").show(truncate=False)
spark.sql("SELECT * FROM cdcstations WHERE trim(name) = 'Kempten'").show(truncate=False)
spark.sql("SELECT bundesland, MAX(hoehe) as maxhoehe FROM cdcstations GROUP BY bundesland ORDER BY maxhoehe DESC").show(truncate=False)
spark.sql("SELECT bundesland, COUNT(*) as anzStationen FROM cdcstations GROUP BY bundesland ORDER BY anzStationen DESC").show(truncate=False)


# 2.)
    
def split_line(line):
    parts = line.split(";")
    parts = [part.strip() for part in parts]
    id_value = int(parts[0])
    datum = datetime.strptime(parts[1][:-2], "%Y%m%d")
    stunde = int(parts[1][-2:].strip())
    qn9 = int(parts[2])
    tt_tu = float(parts[3])
    rf_tu = float(parts[4])
    eor = parts[5]
    
    return (id_value, datum, stunde, qn9, tt_tu, rf_tu, eor)

def import_product_data(scon, spark):
    path = "/data/cdc/hourly/data"
    data_list = []
    
    progress = 0
    for filename in os.listdir(path):
        if filename.endswith(".txt"):
            progress += 1
            file = os.path.join(path, filename)
            data = scon.textFile(file)
            data_filtered = data.zipWithIndex().filter(lambda x: x[1] > 0).map(lambda x: x[0])
            data_list.append(data_filtered)
            
            if progress % 100 == 0:
                print("Progress: " + str(progress) +  " files imported...")

    
    print("Done importing files...")
    all_data = scon.union(data_list)
    all_data_split = all_data.map(split_line)
    
    
    productschema = StructType([
            StructField('station_id', IntegerType(), True),
            StructField('mess_datum', DateType(), True),
            StructField('stunde',   IntegerType(), True),
            StructField('qn_9',    IntegerType(),  True),
            StructField('tt_tu',   FloatType(),  True),
            StructField('rf_tu',   FloatType(),  True),
            StructField('eor', StringType(), True)
        ])
    
    dataframe = spark.createDataFrame(all_data_split, schema=productschema)
    
    dataframe.createOrReplaceTempView("cdcdata")
    
    dataframe.write.mode('overwrite').parquet(CDCPATH + "cdcdata.parquet")
    dataframe.cache()
    print("Imported cdcdata")
    return dataframe


def import_product_data_parallel(scon, spark):
     path = "/data/cdc/hourly/data"
     rdd = scon.textFile(path + "/*.txt")
     rdd_filtered = rdd.zipWithIndex().filter(lambda x: not x[0].startswith("STATIONS_ID")).map(lambda x: x[0])
     rdd_split = rdd_filtered.map(split_line)
     
     productschema = StructType([
        StructField('station_id', IntegerType(), True),
        StructField('mess_datum', DateType(), True),
        StructField('stunde',   IntegerType(), True),
        StructField('qn_9',    IntegerType(),  True),
        StructField('tt_tu',   FloatType(),  True),
        StructField('rf_tu',   FloatType(),  True),
        StructField('eor', StringType(), True)
     ])
     
     dataframe = spark.createDataFrame(rdd_split, schema=productschema)
    
     dataframe.createOrReplaceTempView("cdcdata")
    
     dataframe.write.mode('overwrite').parquet(CDCPATH + "cdcdata.parquet")
     dataframe.cache()
     print("Imported cdcdata")
     return dataframe

import_product_data(scon, spark)
import_product_data_parallel(scon, spark)





spark.sql("SELECT * FROM cdcdata WHERE YEAR(mess_datum) = 2020").show(truncate=False)
spark.sql("SELECT * FROM cdcdata WHERE tt_tu > 0.0 ORDER BY tt_tu DESC").show(truncate=False)
