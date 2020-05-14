from pyspark.sql.functions import concat, col, lit
from pyspark.sql.functions import regexp_replace
from pyspark import SparkContext
from pyspark.sql import SparkSession
import os.path
from os import path
import zipfile
from kaggle.api.kaggle_api_extended import KaggleApi

# Define variables
covid_ds="covid_19_data.csv"
weather_ds="weather.csv"


def get_datasets():
  api = KaggleApi()
  api.authenticate()
  print("Downloading COVID-19 dataset from Kaggle...")
  api.dataset_download_file('sudalairajkumar/novel-corona-virus-2019-dataset',covid_ds)
  print("Downloading weather dataset from Kaggle...")
  api.dataset_download_file('leela2299/weather-covid19',weather_ds)

  print("Unzip dataset files...")
  with zipfile.ZipFile(covid_ds + ".zip","r") as zip_ref:
      zip_ref.extractall(".")
  with zipfile.ZipFile(weather_ds + ".zip","r") as zip_ref:
      zip_ref.extractall(".")

if path.exists(covid_ds) and path.exists(weather_ds):
  print("Datasets already downloaded")
else:
  get_datasets()

#sc = SparkContext("local", "COVID-19 Weather Analysis App")
spark = (SparkSession.builder
  .appName("COVID-19 Weather Analysis App")
  .getOrCreate()
)

sc = spark.sparkContext

cdf=spark.read.csv(covid_ds,inferSchema=True, header = True)
wdf=spark.read.csv(weather_ds,inferSchema=True, header = True)

"""
cdf.printSchema()
wdf.printSchema()
root
 |-- SNo: integer (nullable = true)
 |-- ObservationDate: string (nullable = true)
 |-- Province/State: string (nullable = true)
 |-- Country/Region: string (nullable = true)
 |-- Last Update: string (nullable = true)
 |-- Confirmed: double (nullable = true)
 |-- Deaths: double (nullable = true)
 |-- Recovered: double (nullable = true)

root
 |-- humidity: double (nullable = true)
 |-- country_region: string (nullable = true)
 |-- date: timestamp (nullable = true)
 |-- month: integer (nullable = true)
 |-- day: integer (nullable = true)
 |-- week: integer (nullable = true)
 |-- temp_max_mean_week: double (nullable = true)
 |-- temp_min_mean_week: double (nullable = true)
 |-- mean_week_humidity: double (nullable = true)
"""

cdf.createOrReplaceTempView("covid_t")
wdf.createOrReplaceTempView("weather_t")
"""
spark.sql("SELECT * FROM covid_t").show(5)
spark.sql("SELECT * FROM weather_t").show(5)
OR
Sample Queries:
wdf.select("*").show()
cdf.groupBy("Country/Region").count().show(200)
wdf.groupBy("country_region").count().show(200)
cdf.filter(cdf.Country.like("US%")).show()
wdf.filter(wdf.country_region.like("US%")).show()
cdf.filter(cdf.State.isNotNull()).show()
cdf.filter(cdf.Country.eqNullSafe("Mainland China")).show()
cdf.filter(cdf.Country.eqNullSafe("Mainland China")).groupBy("Country").count().show()
"""

######################
# Data cleansing phase
######################
# To explore data for every data cleansing phase, review the data via queries like "Sample Queries" above
#
# Remove "_unknown" in country name in weather dataset
#   wdf.select("country_region").show() to verify
#   Example: Afghanistan_unknown to Afghanistan
wdf=wdf.withColumn("country_region", regexp_replace("country_region","_unknown",""))

# wdf.groupBy("country_region").count().show(200)
# Rename covid country column to be able to do filter/where operations (remove the "/")
cdf=cdf.withColumnRenamed("Country/Region","Country")
cdf=cdf.withColumnRenamed("Province/State","State")

# Rename "Mainland China" in covid dataset to "China"
#   cdf.filter(cdf.Country.like("%China%")).groupBy("Country").count().show() to verify
cdf=cdf.withColumn("Country", regexp_replace("Country","Mainland ",""))

# Concatenate "Country" and "State" columns in covid table to match country_region column on weather data - in preparation for join 
#   format of <Country>_<State>. ie: China_Anhui
cdf=cdf.withColumn("country_region_c",concat(col("Country"),lit('_'),col("State").isNotNull)).show(false)

spark.stop()
sc.stop()








