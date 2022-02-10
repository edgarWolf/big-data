# Nach spaarkstart.py und ghcnd_stations.py aufrufen
read_ghcnd_from_parquet(spark)


import matplotlib.pyplot as plt

# a)
spark.sql("SELECT * FROM ghcndstations").show(truncate=False)

# b)
spark.sql("SELECT COUNT(*) AS anz, s.countrycode, c.countryname \
          FROM ghcndstations s, ghcndcountries c \
          WHERE s.countrycode = c.countrycode \
          GROUP BY s.countrycode, c.countryname \
          ORDER BY anz DESC").show(truncate=False)
          
          
# c)
spark.sql("SELECT * FROM ghcndstations \
          WHERE countrycode = 'GM' \
          ORDER BY stationname ASC").show(truncate=False)
          
import time
  
# d)

def plotMaxMinTempOverYear(stationName, year):
    
    # Temperatur in Zehntel Grad
    
     sqlMin = "SELECT d.value / 10 AS value FROM ghcnddata d, ghcndstations s \
               WHERE trim(s.stationname) = '{0}' AND \
               d.stationid = s.stationid AND \
               d.year = {1} AND d.element = 'TMIN'".format(stationName, year)

               
     sqlMax = "SELECT d.value / 10 AS value FROM ghcnddata d, ghcndstations s \
               WHERE trim(s.stationname) = '{0}' AND \
               d.stationid = s.stationid AND \
               d.year = {1} AND d.element = 'TMAX'".format(stationName, year)
      
     minValues = spark.sql(sqlMin).toPandas()
     maxValues = spark.sql(sqlMax).toPandas()
      
     fig, ax = plt.subplots()
     ax.plot(minValues["value"], c="blue", label="Minimaltemperatur")
     ax.plot(maxValues["value"], c="red", label="Maximaltemperatur")
     plt.legend()
     plt.title("Max-/Minimaltemperatur in {0} im Jahr {1}".format(stationName, year))
     plt.show()


plotMaxMinTempOverYear("KEMPTEN", 2018)  
plotMaxMinTempOverYear("ZUGSPITZE", 2018) 


# e)

def plotNiederschlaege(stationName):
    start = time.time()
    sql = "SELECT d.year, SUM(d.value) as niederschlag FROM ghcnddata d, ghcndstations s \
           WHERE trim(s.stationname) = '{0}' AND d.stationid = s.stationid AND \
           element = 'PRCP' \
           GROUP BY year ORDER BY year".format(stationName)
    niederschlaege = spark.sql(sql).toPandas()
    plt.bar(niederschlaege["year"], niederschlaege["niederschlag"])
    plt.title("Jährlicher Niederschlag in {0}".format(stationName))
    plt.show()
    
plotNiederschlaege("KEMPTEN")
plotNiederschlaege("ZUGSPITZE")


# f)
def plotTempDist(stationName):
    sql = "SELECT AVG(d.value / 10) AS avgMax, DAYOFYEAR(d.date) AS dayOfYear, \
           AVG(AVG(d.value / 10)) OVER (PARTITION BY d.stationid ORDER BY DAYOFYEAR(d.date) ROWS 20 PRECEDING) AS windowvalue \
           FROM ghcnddata d, ghcndstations s \
           WHERE trim(s.stationname) = '{0}' AND s.stationid = d.stationid \
           AND d.element = 'TMAX' \
           GROUP BY dayOfYear, d.stationid ORDER BY dayOfYear".format(stationName)
    tempDist = spark.sql(sql).toPandas()
    fig, ax = plt.subplots()
    ax.plot(tempDist["dayOfYear"], tempDist["avgMax"], c="b", label="Durschnnitt TMAX")
    ax.plot(tempDist["dayOfYear"], tempDist["windowvalue"], c="r", label="Durschnitt TMAX über 21 Tage")
    plt.legend()
    plt.show()
    
plotTempDist("KEMPTEN")
plotTempDist("ZUGSPITZE")

# g)
def plotAvgTMAXTMIN(stationName):
    # 2021 rausgefiltert, da unzureichende Datenbasis für das gesamte Jahr.
    sub_max_sql = "SELECT d.stationid, d.year, AVG(d.value/10) AS maxAvg \
                         FROM ghcnddata d, ghcndstations s \
                         WHERE trim(s.stationname) = '{0}' \
                         AND s.stationid = d.stationid \
                         AND d.element = 'TMAX' AND d.year < 2021 \ 
                         GROUP BY d.year, d.stationid".format(stationName)
    sub_min_sql = sub_max_sql.replace("TMAX", "TMIN")
    sub_min_sql = sub_min_sql.replace("maxAvg", "minAvg")
    
    sql = "SELECT maxvalue.stationid, maxvalue.year, maxAvg, minAvg \
           FROM ({0}) AS maxvalue, ({1}) AS minvalue \
           WHERE maxvalue.year = minvalue.year \
           ORDER BY maxvalue.year, maxvalue.stationid".format(sub_max_sql, sub_min_sql)
           
    result = spark.sql(sql).toPandas()
    fig, ax = plt.subplots()
    ax.plot(result["year"], result["maxAvg"], c="r", label="Durschnittliche TMAX")
    ax.plot(result["year"], result["minAvg"], c="b", label="Durschnittliche TMIN")
    plt.legend()
    plt.title("Temperaturentwicklung in {0}".format(stationName))
    plt.show()
    
plotAvgTMAXTMIN("KEMPTEN")
plotAvgTMAXTMIN("ZUGSPITZE")
 

# h)
  
def plotAvgTMAX(stationName):
    sql = "SELECT d.year, AVG(value / 10) as avgTMAX, \
           AVG(AVG(d.value / 10)) OVER (PARTITION BY d.stationid ORDER BY d.year ROWS 19 PRECEDING) AS windowvalue \
           FROM ghcnddata d, ghcndstations s \
           WHERE trim(s.stationname) = '{0}' \
           AND s.stationid = d.stationid \
           AND d.element = 'TMAX' AND d.year < 2021\
           GROUP BY d.year, d.stationid ORDER BY d.year".format(stationName)
    result = spark.sql(sql).toPandas()
     
    fig, ax = plt.subplots()
    ax.plot(result["year"], result["avgTMAX"], c="r", label="Durschnittliche TMAX")
    ax.plot(result["year"], result["windowvalue"], c="b", label="Durschnittliche TMAX über 20 Jahre")
    plt.legend()
    plt.title("Temperaturtrend in {0}".format(stationName))
    plt.show()
     
 
plotAvgTMAX("KEMPTEN")


def plotCorrTMAXTMIN(stationName):
    sub_max_sql = "SELECT d.date, d.stationid, (d.value / 10) AS maxTemp \
                   FROM ghcnddata d, ghcndstations s \
                   WHERE trim(s.stationname) = '{0}' \
                   AND s.stationid = d.stationid \
                   AND d.element = 'TMAX'".format(stationName)
    sub_min_sql = sub_max_sql.replace("TMAX", "TMIN")
    sub_min_sql = sub_min_sql.replace("maxTemp", "minTemp")
    
    
    sql = "SELECT max.stationid, YEAR(max.date) as year, corr(max.maxTemp, min.minTemp) AS correlation \
           FROM ({0}) as max, ({1}) as min \
           WHERE max.date = min.date \
           GROUP BY year, max.stationid ORDER BY year, max.stationid".format(sub_max_sql, sub_min_sql)
    result = spark.sql(sql).toPandas()
     
    plt.bar(result["year"], result["correlation"])
    plt.title("Korrelation zwischen TMIN und TMAX in {0}".format(stationName))
    plt.show()
    
plotCorrTMAXTMIN("KEMPTEN")
    
      