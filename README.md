# covid-weather
COVID-19 Weather Analysis

Data Journey
Data Problem Definition
Dataset Discovery
Data Ingest (cleansing, transformations, etc)
Data Analysis
Machine Learning
Reporting/Visualization
Data Archival/Destruction


Data Problem Definition
Understanding the link (if any) between weather and coronavirus cases

Dataset Discovery
Critical to find reliable dataset sources. For this exercise, I went with data from kaggle.com, which is considered fairly reliable, but is not from the actual source data and not rigorously peer-reviewed.
Typically this may involve paying to get the reliable data sources you need

Data Ingest
Use Spark to ingest datasets into Spark RDD's
Will need to perform some cleansing of the data and transformations




Environment
To install Spark locally (on Mac):
brew install scala
brew install apache-spark
Add the following in your profile:
export SPARK_HOME=/usr/local/Cellar/apache-spark/2.4.5/libexec
export PATH=$JAVA_HOME/bin:$SPARK_HOME:$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH

# Install Kaggle API to download datasets
pip install kaggle

# Create Kaggle API token
Go to your Kaggle account at https://www.kaggle.com/<account_name>/account
Click on "Create New API Token"
Move the auto-downloaded json file (kaggle.json) to your Kaggle home directory (/Users/hmahmoud/.kaggle/)
chmod 600 /Users/hmahmoud/.kaggle/kaggle.json
Contents of kaggle.json will look like this
....
{"username":"ihazem","key":"<KEY>"}
....

Assumptions:
* Clean/reliable data source quality
* 


Datasets:
https://www.kaggle.com/sudalairajkumar/novel-corona-virus-2019-dataset
https://www.kaggle.com/leela2299/weather-covid19

References:
https://github.com/Kaggle/kaggle-api/blob/master/kaggle/api/kaggle_api_extended.py
https://technowhisp.com/kaggle-api-python-documentation/
http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.functions
https://spark.apache.org/docs/2.3.0/sql-programming-guide.html
