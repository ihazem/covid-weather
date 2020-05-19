import pyspark_cassandra
from pyspark.sql.functions import to_date
from pyspark.sql.functions import concat, col, lit
from pyspark.sql.functions import regexp_replace
from pyspark import SparkContext
from pyspark.sql import SparkSession
from os import path
import zipfile
from kaggle.api.kaggle_api_extended import KaggleApi



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


def data_analytics(weathercovid_df):
  ######################
  # Data analytics phase
  ######################

  print("Read data from Cassandra table...")

  cw=spark.read.format("org.apache.spark.sql.cassandra").options(table="covidweather", keyspace="hazem").load()

  print("Finished reading data!")

  print("Read in data for Malta...")
  malta = cw.where(cw.country_region == "Malta")

  print("Convert to Pandas DF for charting/visualization...")
  pandas_df = malta.toPandas()

  print("Convert date column to string...")
  pandas_df['date'] = pd.to_datetime(pandas_df.date)
  pandas_df['date'] = pandas_df['date'].dt.strftime('%Y-%m-%d')

  plt.scatter(pandas_df.date, pandas_df.confirmed, pandas_df.temp_min_mean_week)

  

def data_ingest(weathercovid_df):
  ######################
  # Data ingest phase
  ######################

  print("Ingest/write denormalized data into Cassandra...")

  # TO DO - CREATE CASSANDRA KEYSPACE/TABLE FROM HERE and NOT CQLSH
  weathercovid_df.show()
  weathercovid_df.write.format("org.apache.spark.sql.cassandra").mode('append').options(table="covidweather", keyspace="hazem").save()

  print("Finished data ingestion phase!")

def data_cleanse(wdf, cdf):
  ######################
  # Data cleansing phase
  ######################

  print("Begin data cleansing and transformation phase...")

  # To explore data for every data cleansing phase, review the data via queries like "Sample Queries" above
  #
  # Remove "_unknown" in country name in weather dataset
  #   wdf.select("country_region").show() to verify
  #   Example: Afghanistan_unknown to Afghanistan
  wdf=wdf.withColumn("country_region", regexp_replace("country_region","_unknown",""))

  # Format weather dataset date/timestamp to match covid dataset date/time
  #   Example: 2020-02-01 00:00:00 to 2020-02-01
  wdf=wdf.withColumn("date", to_date(wdf.date, "MM/dd/yyyy"))

  # Drop unnecessary columns before importing to Cassandra
  w_columns_to_drop = ['month', 'day', 'week']
  wdf=wdf.drop(*w_columns_to_drop)

  # Rename covid country column to be able to do filter/where operations (remove the "/")
  cdf=cdf.withColumnRenamed("Country/Region","Country")
  cdf=cdf.withColumnRenamed("Province/State","State")

  # Rename "Mainland China" in covid dataset to "China"
  #   cdf.filter(cdf.Country.like("%China%")).groupBy("Country").count().show() to verify
  cdf=cdf.withColumn("Country", regexp_replace("Country","Mainland ",""))

  # Remove all null values in State column
  cdf=cdf.na.fill("")

  # Concatenate "Country" and "State" columns in covid table to match country_region column on weather data - in preparation for join 
  #   format of <Country>_<State>. ie: China_Anhui
  cdf=cdf.withColumn("country_region_c",concat(col("Country"),lit('_'),col("State")))

  # Remove trailing underscore "_" from concatenation/merging of columns above that did not have a state
  #   Example: Japan_ to Japan
  cdf=cdf.withColumn("country_region_c", regexp_replace("country_region_c","_$",""))

  # To find and remove where Country and State columns are equal
  # cdf.filter(cdf.Country.eqNullSafe(cdf.State)).groupBy("country_region_c").count().show()
  #   Example: "Hong Kong_Hong Kong" to "Hong Kong"
  # However, turns out both datasets have that (ie: France_France), so no need to filter it.
  
  # Convert date string in covid dataset to timestamp data type with specific formatting 
  cdf=cdf.withColumn("ObservationDate", to_date(cdf.ObservationDate, "MM/dd/yyyy"))

  # Drop unnecessary columns before importing to Cassandra
  c_columns_to_drop = ['SNo', 'State', 'Country', 'Last Update']
  cdf=cdf.drop(*c_columns_to_drop)

  print("Denormalize data...")
  inner_j = cdf.join(wdf, (cdf.country_region_c == wdf.country_region) & (cdf.ObservationDate == wdf.date))
  print("Finished data denormalization")

  # Drop duplicate date column (keep "date" and "country_region")
  j_columns_to_drop = ['ObservationDate', 'country_region_c']
  inner_j=inner_j.drop(*j_columns_to_drop)

  # Rename columns so as to not deal with Cassandra's case sensitivity on CQLSH
  inner_j=inner_j.withColumnRenamed("Confirmed","confirmed")
  inner_j=inner_j.withColumnRenamed("Deaths","deaths")
  inner_j=inner_j.withColumnRenamed("Recovered","recovered")
  inner_j=inner_j.withColumnRenamed("Recovered","recovered")

  return inner_j

  print("Finished data cleansing phase!")



# Define variables
covid_ds="covid_19_data.csv"
weather_ds="weather.csv"

# Check if datasets have been downloaded
if path.exists(covid_ds) and path.exists(weather_ds):
  print("Datasets already downloaded")
else:
  get_datasets()

#sc = SparkContext("local", "COVID-19 Weather Analysis App")
spark = (SparkSession.builder
  .appName("COVID-19 Weather Analysis App")
  .getOrCreate()
)

print("Read in datasets into Spark DataFrame...")
cdf=spark.read.csv(covid_ds,inferSchema=True, header = True)
wdf=spark.read.csv(weather_ds,inferSchema=True, header = True)

sc = spark.sparkContext


weathercovid_df = data_cleanse(wdf, cdf)
data_ingest(weathercovid_df)
data_analytics(weathercovid_df)

spark.stop()
sc.stop()
