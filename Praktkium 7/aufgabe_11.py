#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec  7 22:07:08 2021

@author: wolfedgar
"""

from matplotlib import pyplot as plt

# a)

def create_aggregated_data_frame(stationName):
    
    sql = """SELECT min(d.tt_tu) AS minTemp, max(d.tt_tu) AS maxTemp, avg(d.tt_tu) AS avgTemp,
             YEAR(d.mess_datum) AS jahr, d.stunde AS stunde,           
             DATE(STRING(YEAR(d.mess_datum)) || '-' || STRING(QUARTER(d.mess_datum)*3-2) || '-01') AS quartal,
             DATE(STRING(YEAR(d.mess_datum)) || '-' || STRING(MONTH(d.mess_datum))) AS monat, 
             DATE(STRING(YEAR(d.mess_datum)) || '-' || STRING(MONTH(d.mess_datum)) || '-' || STRING(DAY(d.mess_datum))) AS tag
             FROM cdcstations s, cdcdata d
             WHERE s.id = d.station_id AND trim(s.name) = '{0}' AND d.tt_tu > -999
             GROUP BY ROLLUP(jahr, quartal, monat, tag, stunde) 
             ORDER BY jahr, quartal, monat, tag, stunde
             """.format(stationName)
    dataframe = spark.sql(sql)
    table_name = "cdcAggvalues" + stationName
    dataframe.createOrReplaceTempView(table_name)
    dataframe.cache()
    
    print("Cached aggregated values for station {0}. Use {1} as table name.".format(stationName, table_name))
    return dataframe

create_aggregated_data_frame("Kempten")


def plot_quarter_values():
    sql = "SELECT minTemp, maxTemp, avgTemp, quartal, jahr \
           FROM cdcAggvaluesKempten WHERE jahr >= 2000 AND jahr <= 2020 \
           AND monat IS NULL AND tag IS NULL AND stunde IS NULL AND quartal is NOT NULL \
           ORDER BY jahr"
    result = spark.sql(sql).toPandas()
    fig, ax = plt.subplots()
    ax.plot(result["quartal"], result["minTemp"])
    ax.plot(result["quartal"], result["avgTemp"])
    ax.plot(result["quartal"], result["maxTemp"])
    plt.title("Quartalswerte in Kempten von 2000 - 2020")
    plt.show()

plot_quarter_values()
    

def plot_yearly_values():
    sql = "SELECT minTemp, maxTemp, avgTemp, jahr \
           FROM cdcAggvaluesKempten WHERE jahr >= 2000 AND jahr <= 2020 \
           AND monat IS NULL AND tag IS NULL AND stunde IS NULL AND quartal IS NULL \
           ORDER BY jahr"
    result = spark.sql(sql).toPandas()
    fig, ax = plt.subplots()
    ax.plot(result["jahr"], result["minTemp"])
    ax.plot(result["jahr"], result["avgTemp"])
    ax.plot(result["jahr"], result["maxTemp"])
    plt.title("Jahreswerte in Kempten von 2000 - 2020")
    plt.show()
    
plot_yearly_values()


def plot_daily_values():
    sql = "SELECT minTemp, maxTemp, avgTemp, jahr, tag\
           FROM cdcAggvaluesKempten WHERE jahr >= 2017 AND jahr <= 2020 \
           AND tag is NOT NULL AND stunde IS NULL \
           ORDER BY tag"
    result = spark.sql(sql).toPandas()
    fig, ax = plt.subplots(figsize=[9, 6])
    ax.plot(result["tag"], result["minTemp"])
    ax.plot(result["tag"], result["avgTemp"])
    ax.plot(result["tag"], result["maxTemp"])
    plt.title("Tageswerte in Kempten von 2017 - 2020")
    plt.tight_layout()
    plt.show()
    
plot_daily_values()

def plot_monthly_values():
    sql = "SELECT minTemp, maxTemp, avgTemp, monat \
           FROM cdcAggvaluesKempten WHERE jahr >= 2000 AND jahr <= 2020 \
           AND monat IS NOT NULL AND tag IS NULL AND stunde IS NULL AND minTemp > -999 \
           ORDER BY monat"
    result = spark.sql(sql).toPandas()
    fig, ax = plt.subplots()
    ax.plot(result["monat"], result["minTemp"])
    ax.plot(result["monat"], result["avgTemp"])
    ax.plot(result["monat"], result["maxTemp"])
    plt.title("Monatswerte in Kempten von 2000 - 2020")
    plt.show()
    
plot_monthly_values()


# b)
def create_table_tempmonat():
    sql = """SELECT MAX(d.tt_tu) AS maxTemp, MIN(d.tt_tu) AS minTemp, AVG(d.tt_tu) AS avgTemp,
           s.name AS stationName, s.id as stationid, YEAR(d.mess_datum) AS jahr,
           DATE(STRING(YEAR(d.mess_datum)) || '-' || STRING(MONTH(d.mess_datum))) AS monat
           FROM cdcdata d, cdcstations s
           WHERE s.id = d.station_id
           AND d.tt_tu > -999
           GROUP BY stationid, stationName, jahr, monat
           ORDER BY jahr, monat"""
    dataframe = spark.sql(sql)
    dataframe.createOrReplaceTempView("tempmonat")
    dataframe.cache()
    return dataframe
    
create_table_tempmonat()
    
# i ) 
def rank_temp_in_2015(tempSelect):
    sql = """SELECT {}, monat, stationname, 
             RANK() OVER (PARTITION BY monat ORDER BY {}) AS rang
             FROM tempmonat WHERE jahr = 2015
             ORDER BY rang, {}""".format(tempSelect, tempSelect, tempSelect)
    spark.sql(sql).show(truncate=False)
    
rank_temp_in_2015("minTemp")
rank_temp_in_2015("avgTemp")
rank_temp_in_2015("maxTemp")
    

# ii )
def rank_coldest_temp_over_years(tempSelect):
    sql = """SELECT {}, monat, jahr, stationname, 
              RANK() OVER (ORDER BY {}) AS rang
              FROM tempmonat
              ORDER BY rang, jahr, {}""".format(tempSelect, tempSelect, tempSelect)
    spark.sql(sql).show(truncate=False)
    
rank_coldest_temp_over_years("minTemp")
rank_coldest_temp_over_years("avgTemp")
rank_coldest_temp_over_years("maxTemp")

# c)

def create_groupingsets_table():
    sql = """SELECT MIN(d.tt_tu) as minTemp, AVG(d.tt_tu) as avgTemp, MAX(d.tt_tu) as maxTemp,
             s.bundesland AS bundesland, YEAR(d.mess_datum) AS jahr,  
             MONTH(d.mess_datum) AS monat, s.name as stationName, s.id as stationID
             FROM cdcstations s, cdcdata d 
             WHERE s.id = d.station_id AND d.tt_tu > -999
             GROUP BY GROUPING SETS( 
                 (YEAR(d.mess_datum), bundesland), (YEAR(d.mess_datum), s.id, s.name), 
                 (MONTH(d.mess_datum), bundesland) )"""
    dataframe = spark.sql(sql)
    dataframe.createOrReplaceTempView("groupingSetsTable")
    dataframe.cache()
    return dataframe
    
create_groupingsets_table()

def select_by_bundesland_jahr():
    sql = """SELECT minTemp, avgTemp, maxTemp, jahr, bundesland
             FROM groupingSetsTable 
             WHERE jahr IS NOT NULL AND bundesland IS NOT NULL
             ORDER BY minTemp"""
    spark.sql(sql).show(truncate=False)
    
select_by_bundesland_jahr()

def select_by_station_jahr():
    sql = """SELECT minTemp, avgTemp, maxTemp, jahr, stationName, stationID
             FROM groupingSetsTable 
             WHERE jahr IS NOT NULL AND stationName IS NOT NULL
             ORDER BY minTemp"""
    spark.sql(sql).show(truncate=False)
    
select_by_station_jahr()


def select_by_bundesland_month():
    sql = """SELECT minTemp, avgTemp, maxTemp, monat, bundesland
             FROM groupingSetsTable 
             WHERE monat IS NOT NULL AND bundesland IS NOT NULL
             ORDER BY minTemp"""
    spark.sql(sql).show(truncate=False)
    
select_by_bundesland_month()

