HDFSPATH = "hdfs://193.174.205.250:54310/"
STOCKSPATH = "stocks/"
STOCKSFILE = "stocks.parquet"
PORTFOLIOPATH = "portfolio.parquet"

def read_stocks_from_parquet(spark):
    stocksDf = spark.read.parquet(HDFSPATH + STOCKSPATH + STOCKSFILE)
    stocksDf.createOrReplaceTempView("stocks")
    stocksDf.cache()
    
    portfoilioDf = spark.read.parquet(HDFSPATH + STOCKSPATH + PORTFOLIOPATH)
    portfoilioDf.createOrReplaceTempView("portfolios")
    portfoilioDf.cache()
    
read_stocks_from_parquet(spark)



# a)

def statement_a():
    sql = """SELECT MIN(dt) as minDate, MAX(dt) AS maxDate, symbol 
             FROM stocks 
             GROUP BY symbol
             ORDER BY symbol"""
    spark.sql(sql).show()
    
statement_a()


# b)
def statement_b():
    sql = """SELECT MIN(close) as minnClose, MAX(close) AS maxClose, AVG(close) AS avgClose, symbol
             FROM stocks 
             WHERE YEAR(dt) = 2009
             GROUP BY symbol
             ORDER BY symbol"""
    spark.sql(sql).show()
    
statement_b()


# c)
def statement_c():
    sql = """SELECT b.symbol AS symbol, SUM(b.num) AS num
             FROM portfolios
             LATERAL VIEW explode(bonds) AS b
             GROUP BY symbol
             ORDER BY symbol"""
    spark.sql(sql).show()
    
statement_c()


# d)

def statenent_d():     
    sql = """SELECT DISTINCT b.symbol as symbol 
             FROM portfolios
             LATERAL VIEW explode(bonds) AS b
             EXCEPT SELECT DISTINCT symbol FROM stocks"""
    spark.sql(sql).show()
    
statenent_d()


# e)
def statement_e():
    symbols_end_2010 =  """SELECT symbol, MAX(dt) AS endDate
                            FROM stocks 
                            WHERE YEAR(dt) = 2010
                            GROUP BY symbol"""
                            
    close_symobls_2010 = """SELECT s.symbol, s.dt, s.close
                            FROM stocks s, ({}) AS tempSymbs
                            WHERE s.symbol = tempSymbs.symbol 
                            AND s.dt = tempSymbs.endDate""".format(symbols_end_2010)
                            
    sql = """SELECT p.pid, SUM(closeSymbs.close * b.num) as pfValue
             FROM ({}) AS closeSymbs, portfolios p LATERAL VIEW explode(bonds) AS b
             WHERE closeSymbs.symbol = b.symbol
             GROUP BY p.pid
             ORDER BY p.pid""".format(close_symobls_2010)
    spark.sql(sql).show()
    
statement_e()
