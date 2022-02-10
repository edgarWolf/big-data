#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov 29 20:50:22 2021

@author: wolfedgar
"""

from matplotlib import pyplot as plt

# a)

def plot_all_stations():
    sql = "SELECT breite, laenge FROM cdcstations"
    result = spark.sql(sql).toPandas()
    plt.scatter(result["laenge"], result["breite"], s=5)
    plt.tight_layout()
    plt.show()
    
plot_all_stations()

# b)

def plot_all_stations_by_time():
    sql = "SELECT breite, laenge, \
          (YEAR(zu_datum) - YEAR(von_datum)) AS op_time FROM cdcstations"
    result = spark.sql(sql).toPandas()
    plt.scatter(result["laenge"], result["breite"], s=result["op_time"])
    plt.tight_layout()
    plt.show()
    
    
plot_all_stations_by_time()

# c)
def plot_frostdays_for_given_year(year):
    maxTempSql = "SELECT d.station_id AS station_id, YEAR(d.mess_datum) AS year, d.mess_datum as mess_datum, MAX(d.tt_tu) AS maxTemp \
                  FROM cdcdata d \
                  WHERE YEAR(mess_datum) = {0} \
                  GROUP BY station_id, mess_datum ORDER BY mess_datum".format(year)
                  
    frostDaysSql = "SELECT maxTemps.station_id, COUNT(*) as frostDays \
                    FROM ({0}) AS maxTemps WHERE maxTemps.maxTemp < 0 \
                    GROUP BY maxTemps.station_id \
                    ORDER BY frostDays".format(maxTempSql)
                    
    station_sql = "SELECT stationCount.frostDays AS frostDays, COUNT(*) AS anzStations \
                   FROM ({0}) AS stationCount \
                   GROUP BY frostDays ORDER BY frostDays".format(frostDaysSql)
    result = spark.sql(station_sql).toPandas()
    plt.bar(result["frostDays"], result["anzStations"])
    plt.title("Anzahl Frosttage im Jahr {0}".format(year))
    plt.show()
    
    
plot_frostdays_for_given_year(2012)


def plot_frostdays_for_station_per_year(stationName):
    maxTempSql = "SELECT YEAR(d.mess_datum) AS year, d.mess_datum as mess_datum, MAX(d.tt_tu) AS maxTemp \
                  FROM cdcdata d, cdcstations s \
                  WHERE d.station_id = s.id AND trim(s.name) = '{0}' \
                  GROUP BY year, mess_datum ORDER BY mess_datum".format(stationName)
    
    
    count_sql = "SELECT stationTemp.year AS year, COUNT(*) AS frostDays \
                 FROM ({0}) AS stationTemp \
                 WHERE stationTemp.maxTemp < 0 \
                 GROUP BY year ORDER BY year".format(maxTempSql)
    # spark.sql(count_sql).show(truncate=False)
    
    result = spark.sql(count_sql).toPandas()
    plt.bar(result["year"], result["frostDays"])
    plt.show()
    
plot_frostdays_for_station_per_year("Kempten")



# d)

def plot_corr_stationheight_frostdays_per_year():
    maxTempHeightSql = "SELECT s.hoehe AS hoehe, YEAR(d.mess_datum) AS year, d.mess_datum AS mess_datum, MAX(d.tt_tu) AS maxTemp \
                        FROM cdcstations s, cdcdata d \
                        WHERE d.station_id = s.id \
                        GROUP BY mess_datum, hoehe"
    
    frostHeightSql = "SELECT maxTemps.year AS year, maxTemps.hoehe AS hoehe, COUNT(*) AS frostDays \
                      FROM ({0}) AS maxTemps WHERE maxTemps.maxTemp < 0 \
                      GROUP BY year, hoehe".format(maxTempHeightSql)
                      
    corrSql = "SELECT frostCount.year AS year, corr(frostCount.frostDays, frostCount.hoehe) as corr \
               FROM ({0}) AS frostCount \
               GROUP BY year ORDER BY year".format(frostHeightSql)
    result = spark.sql(corrSql).toPandas()
    result.dropna(inplace=True)
    
    plt.bar(result["year"], result["corr"])
    plt.tight_layout()
    plt.show()
    
plot_corr_stationheight_frostdays_per_year()
