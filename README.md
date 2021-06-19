# US Domestic Tourism Data Analytics
### Udacity Data Engineering Capstone Project

#### Project Summary
This project aims to gather data from 3 different datasources related to US immigration and tourism data, and transform them into a star schema with tables designed to optimize queries on the data. The main goal is to provide trends and insights about the volume of trips and the time of the year.
      
The project follows the following steps:
* Step 1: Scope the Project and Gather Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data
* Step 5: Complete Project Write Up
* Extra: Write a few analytical queries


```python
# Do all imports and installs here
import pandas as pd
import datetime as dt
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, StructType, StructField, IntegerType, StringType, FloatType
spark = SparkSession.builder \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
    .getOrCreate()
```

### Step 1: Scope the Project and Gather Data

#### Scope 
*Explain what you plan to do in the project in more detail:*
 - Ingest 3 different datasets related to US Immigration Data, Airlines and Global Temperature
 - Explore and asses the data using this Notebook
 - Run data quality checks and exclude data that won't be used
 - Model a star schema to store the final data model 
 - Create an ETL to read the raw data, transform it and load it into a trusted zone area in a datlake.
 - The analytical tables in the trusted zone will be stored as parquet files
 - Everything will be ready to be loaded into a DW, if necessary
 - Provide a few analytical queries to validate the final tables
 - **The main purpose of the final data model is to esily provide insights about tourists and immigrants behaviours, if it's possible to correlate travels with the time of the year or with certain regions.**

*What data do you use?*
 - I94 Immigration data of 2016
 - World Temperature Data
 - World Airports Data

*What is your end solution look like?*
 - 8 tables stored in a star schema designed to optimize queries on immigration data in US
 - Some analytical queries to validate a few behaviours

*What tools did you use?*
 - Python 3.6
 - PySpark 2.4.3 using Scala version 2.11.12
 - Java HotSpot(TM) 64-Bit Server VM, 1.8.0_291
 - packages in requirements.txt
 

### Describe and Gather Data 
*Describe the data sets you're using. Where did it come from? What type of information is included?*

#### I94 Immigration Data
This data comes from the US National Tourism and Trade Office. Basically, this dataset contains international visitor arrival statistics of the year 2016. 

The original dataset was in sas7bdat format, but for this project, it was transformed into parquet files since it's a smaller size and also for performance purposes when reading it into Spark dataframes.

You can read more about it [here](https://www.trade.gov/national-travel-and-tourism-office).


```python
immig_df = spark.read.parquet("./data/raw/i94-sample")
```

#### World Temperature Data
This dataset came from Kaggle. 

The original dataset was in CSV format, but for this project, it was transformed into parquet files since it's a smaller size and also for performance purposes when reading it into Spark dataframes.

You can read more about it [here](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data).


```python
temp_df = spark.read.parquet("./data/raw/global_temperature")
```

#### Port of Entry Codes
This dataset contains the three-letter codes that are used by Customs and Border Protection (CBP) in its internal communications to represent ports-of-entry (POEs). It is used in the i94 immigration data in the i94port field. This data came maily from [here](https://fam.state.gov/fam/09FAM/09FAM010205.html), but also some data was merged from the file I94_SAS_Labels_Descriptions.SAS, provided by Udacity.


```python
ports_df = spark.read.options(header='True', inferSchema='True', delimiter=';')\
    .csv("./data/raw/port-of-entry-codes.csv")
```

#### Country Codes Table
This is a simple dictionary with the codes used in I94 Forms and the corresponding country. This information was obtained from the file I94_SAS_Labels_Descriptions.SAS, provided by Udacity.


```python
countries_df = spark.read.options(header='True', inferSchema='True', delimiter=';')\
    .csv("./data/raw/country_codes.txt")
```

#### Airline Database
This dataset came from Kaggle. It contains 5888 airline companies.

You can read more about it [here](https://www.kaggle.com/open-flights/airline-database).


```python
airlines_df = spark.read.options(header='True', inferSchema='True', delimiter=',')\
    .csv("./data/raw/airlines.csv")
```

#### Visa Types
This dataset was assembled in a csv file with data from the Department of State - Bureau of Consular Affairs.

You can read more about it [here](https://travel.state.gov/content/travel/en/us-visas/visa-information-resources/all-visa-categories.html).


```python
visa_df = spark.read.options(header='True', inferSchema='True', delimiter=',')\
    .csv("./data/raw/visa-types.csv")
```

### Step 2: Explore and Assess the Data
#### Explore the Data 
*Identify data quality issues, like missing values, duplicate data, etc.*


```python
# assessing the data using SQL:
## attempting to join immigration data with other datasets
immig_df.createOrReplaceTempView("immig_data")
countries_df.createOrReplaceTempView("country_codes")
airlines_df.createOrReplaceTempView("airlines")
ports_df.createOrReplaceTempView("ports")

epoch = dt.datetime(1960, 1, 1).date()
spark.udf.register("isoformat", lambda x: (epoch + dt.timedelta(x)).isoformat() if x else None)
spark.sql('''
          SELECT int(i.cicid) id,
                 i.i94port port,
                 split(p.location, ',')[0] port_city,
                 split(p.location, ',')[1] port_state,
                 isoformat(int(i.arrdate)) arrival_date,
                 isoformat(int(i.depdate)) departure_date,
                 int(i.depdate - i.arrdate) days,
                 CASE
                     WHEN INT(i.i94mode) = 1 THEN 'Air'
                     WHEN INT(i.i94mode) = 2 THEN 'Sea'
                     WHEN INT(i.i94mode) = 3 THEN 'Land'
                     ELSE 'Not reported'
                 END AS mode,
                 CASE
                     WHEN INT(i.i94visa) = 1 THEN 'Business'
                     WHEN INT(i.i94visa) = 2 THEN 'Pleasure'
                     ELSE 'Student'
                 END AS visa,
                 i.visatype,
                 int(i.i94bir) age,
                 i.gender,
                 i.airline,
                 a.name airline_name,
                 i.fltno flight_no,
                 c.country,
                 i.occup
            FROM immig_data i, 
                 country_codes c,
                 airlines a,
                 ports p
           WHERE i.gender IS NOT NULL
             AND int(i.i94res) = c.code
             AND c.country NOT LIKE '%INVALID%'
             AND c.country NOT LIKE '%No Country Code%'
             and a.IATA = i.airline
             and p.code = i.i94port
           LIMIT 50
    ''').toPandas()
```


```python
## assesssing visa data from i94 data
spark.sql('''
    SELECT i94visa, count(cicid)
      FROM immig_data
     GROUP by i94visa
     ORDER BY count(cicid) desc
     LIMIT 50
    ''').toPandas()

spark.sql('''
    SELECT visatype, count(cicid)
      FROM immig_data
     GROUP by visatype
     ORDER BY visatype desc
     LIMIT 20
    ''').toPandas()
```


```python
# assessing port-of-entry data

# check how many differents ports we have in the i94 data
immig_df.createOrReplaceTempView("immig_data")
ports_count = spark.sql('''
    SELECT DISTINCT(I94PORT)
      FROM immig_data
    ''').count()
print(f'ports in i94 data: {ports_count}') #397

# check if all possible ports are in the ports dataframe
ports_df.createOrReplaceTempView("ports")

ports_count = spark.sql('''
    SELECT code
      FROM ports
     WHERE code IN (SELECT DISTINCT(I94PORT)
                      FROM immig_data)
    ''').count()
print(f'matches with ports in CBP data: {ports_count}') #396

#missing port
spark.sql('''
    SELECT *
      FROM immig_data
     WHERE I94PORT NOT IN (SELECT code
                           FROM ports)
    ''').toPandas()
```


```python
# assessing temperature dataset
# selecting cities in US only
# grouping by month to get the average temperature
temp_df.createOrReplaceTempView("temp")
spark.sql('''
    SELECT city, country, month(dt) month, avg(averagetemperature) avg_temp
      FROM temp
     WHERE lower(country) in ('united states')
       AND averagetemperature is not null
     GROUP BY city, country, month(dt)
     ORDER BY city, country, month(dt)
    ''').toPandas()
```


```python
# top 100 airports with the highest immigration records
immig_df.createOrReplaceTempView("immig_data")
top_ports = spark.sql('''

    SELECT I94PORT, count(cicid) cnt
      FROM immig_data
     GROUP BY I94PORT
     ORDER BY COUNT(cicid) desc
    ''')
top_ports.createOrReplaceTempView("top_ports")

#cleaning ports outside us or invalid codes
clean_ports = spark.sql('''
    SELECT code, 
           split(location, ',')[0] city, 
           trim(split(location, ',')[1]) state
      FROM ports
    ''')
clean_ports.createOrReplaceTempView("clean_ports")

clean_ports = spark.sql('''
    SELECT *
      FROM clean_ports cp, top_ports tp
     WHERE cp.code = tp.i94port
       AND cp.state != ''
       and length(cp.state) <= 3
     ORDER by cnt desc
     LIMIT 100
    ''')

```


```python
clean_ports.createOrReplaceTempView("clean_ports")
# port cities with a match in the temperature dataset
spark.sql('''
    SELECT *
      FROM clean_ports p
     WHERE p.city in (SELECT DISTINCT(UPPER(city)) 
                        FROM temp 
                       WHERE lower(country) in ('united states'))
 order by p.cnt desc

    ''').toPandas()

# port cities without a match in the temperature dataset
spark.sql('''
    SELECT *
      FROM clean_ports p
     WHERE p.city not in (SELECT DISTINCT(UPPER(city)) 
                            FROM temp 
                           WHERE lower(country) in ('united states'))
 order by p.cnt desc

    ''').toPandas()
```

#### Cleaning Steps
Steps necessary to clean the data for each dataset are in the comments.


```python
#immigration data

#joining with countries_df and removing invalid rows
immig_df.createOrReplaceTempView("immig_data")
countries_df.createOrReplaceTempView("country_codes")

rows_before = immig_df.count()
print(f'rows count: {rows_before}')

immig_df = spark.sql('''
    SELECT i.*, c.country
      FROM immig_data i,
           country_codes c
     WHERE int(i.i94res) = c.code
       AND c.country NOT LIKE '%INVALID%'
       AND c.country NOT LIKE '%No Country Code%'
    ''')

rows_after = immig_df.count()
print(f'{rows_after - rows_before} invalid rows removed')

#dropping columns that won't be used in this project
cols = ['i94yr', 'i94mon', 'i94cit', 'i94addr', 
        'count', 'dtadfile', 'visapost', 'entdepa', 
        'entdepd', 'entdepu', 'biryear', 'dtaddto', 
        'i94res', 'matflag']
immig_df = immig_df.drop(*cols)

#removing duplicates
print('removing duplicates')
immig_df.dropDuplicates()
print(f'rows count after de-duplicate: {immig_df.count()}')

#casting data types, relabeling column names and replacing values
immig_df.createOrReplaceTempView("immig_data")
epoch = dt.datetime(1960, 1, 1).date()
spark.udf.register("isoformat", lambda x: (epoch + dt.timedelta(x)).isoformat() if x else None)
immig_df = spark.sql('''
    SELECT int(i.cicid) id,
           i.i94port airport,
           isoformat(int(i.arrdate)) arrival_date,
           isoformat(int(i.depdate)) departure_date,   
           i.i94mode mode,
           i.i94visa visa,
           i.visatype,
           int(i.i94bir) age,
           i.gender,
           i.airline,
           i.fltno flight_num,
           i.occup occupation,
           i.admnum admission_num,
           i.country origin_country
      FROM immig_data i
    ''')

print(f'rows count: {immig_df.count()}')
```


```python
# ports data

# split the location field into city and state
# select only the ports that has a match in i94 data

ports_df.createOrReplaceTempView("ports")
ports_df = spark.sql('''
    SELECT code, 
           split(location, ',')[0] city,
           split(location, ',')[1] state
      FROM ports a
     WHERE code IN (SELECT DISTINCT(I94PORT)
                      FROM immig_data)
    ''')
```


```python
# temperature data

# selecting cities in US only
# grouping by month to get the average temperature
temp_df.createOrReplaceTempView("temp")
temp_df = spark.sql('''
    SELECT city, month(dt) month, avg(averagetemperature) avg_temp
      FROM temp
     WHERE lower(country) in ('united states')
       AND averagetemperature is not null
     GROUP BY city, country, month(dt)
     ORDER BY city, country, month(dt)
    ''')
```


```python
# airlines data

# filter out invalid rows
# rename columns
# select only the ones in i94 immigration data

airlines_df.createOrReplaceTempView('airlines')
airlines_df = spark.sql('''
    SELECT a.name,
           a.iata code,
           a.country
      FROM airlines a
     WHERE a.iata is not null
       AND a.iata != '-'
       AND a.iata IN (SELECT DISTINCT(airline)
                      FROM immig_data)
    ''')
```

### Step 3: Define the Data Model
#### 3.1 Conceptual Data Model
*Map out the conceptual data model and explain why you chose that model*

The data model consists in a star schema with 1 fact table and 7 dimension tables:

##### Fact
1. **arrivals** - transformed data from i94 2016 immigration data
 - id, airport, arrival_date, departure_date, mode, visa, visatype, age, gender, airline, flight_num, occupation, admission_num, origin_country

##### Dimensions 
2. **visa** - visa types details
  - type, purpose, code, description


3. **i94mode** - arrival mode
  - code, desc


4. **calendar** - date dimension
  - date, day, week_day, month, month_name, quarter, year, season, season_name


5. **ports** - ports of entry codes and cities
  - code, city, state


6. **airlines** - airlines details
  - name, code, country


7. **countries** - countries codes and names
  - code, country


8. **temperatures** - us cities average temperatures
  - city, month, avg_temp 



#### 3.2 Mapping Out Data Pipelines
*List the steps necessary to pipeline the data into the chosen data model*

1. Ingest all raw datasources into pyspark dataframes
2. Transform the data using pyspark functions and SQL
3. Load the validated and transformed data into the star schema tables, in the datalake trusted zone

### Step 4: Run Pipelines to Model the Data 
#### 4.1 Create the data model
*Build the data pipelines to create the data model.*

The pipeline was already partially develop in this notebook, more specifically:

1. Ingest all raw datasources into pyspark dataframes
 - was already done in Step 1


2. Transform the data using pyspark functions and SQL
 - was partially done in Step 2, missing only the creation of the last two dimension tables: calendar and i94mode. These two tables are created in the next two steps:


```python
# calendar dim table
df = pd.DataFrame({'date': pd.date_range('2000-01-01', '2050-12-31')})
df['day'] = df.date.dt.day
df['week_day'] = df.date.dt.day_name()
df['month'] = df.date.dt.month
df['month_name'] = df.date.dt.month_name()
df['quarter'] = df.date.dt.quarter
df['year'] = df.date.dt.year
df['season'] = df.date.dt.month%12 // 3 + 1
di = {1: 'winter', 2: 'spring', 3: 'summer', 4: 'fall'}
df['season_name'] = df['season'].map(di)

cal_schema = StructType([StructField('date', DateType(), True), \
                         StructField('day', IntegerType(), True), \
                         StructField('week_day', StringType(), True), \
                         StructField('month', IntegerType(), True), \
                         StructField('month_name', StringType(), True), \
                         StructField('quarter', IntegerType(), True), \
                         StructField('year', IntegerType(), True), \
                         StructField('season', IntegerType(), True),
                         StructField('season_name', StringType(), True)])
calendar_df = spark.createDataFrame(df, schema=cal_schema) 
```


```python
# i94mode dim table
columns = ['code','description']
data = [(1, 'Air'), (2, 'Sea'), (3, 'Land'), (9, 'Not Reported')]
rdd = spark.sparkContext.parallelize(data)
i94mode_df = rdd.toDF(columns)
```

Now, as the last part of our ETL, we need to load all dataframes into parquet files in the datalake trusted zone:


```python
#arrivals
immig_df.write.parquet('./data/trusted/arrivals/', mode='overwrite')

#visa
visa_df.write.parquet('./data/trusted/visa/', mode='overwrite')

#i94mode
i94mode_df.write.parquet('./data/trusted/i94mode/', mode='overwrite')

#calendar
calendar_df.write.parquet('./data/trusted/calendar/', mode='overwrite')

#ports
ports_df.write.parquet('./data/trusted/ports/', mode='overwrite')

#airlines
airlines_df.write.parquet('./data/trusted/airlines/', mode='overwrite')

#countries
countries_df.write.parquet('./data/trusted/countries/', mode='overwrite')

#temperatures
temp_df.write.parquet('./data/trusted/temperatures/', mode='overwrite')
```

#### 4.2 Data Quality Checks
*Explain the data quality checks you'll perform to ensure the pipeline ran as expected.*

 - Check if the dataframes have data after the transformations were performed.
 - Check if each table was created successfully as parquet files in its specific directory.

*Run Quality Checks:*


```python
import os.path

tables = [('arrivals', 'immig_df'),
          ('visa', 'visa_df'),
          ('i94mode', 'i94mode_df'),
          ('calendar', 'calendar_df'),
          ('ports', 'ports_df'),
          ('airlines', 'airlines_df'),
          ('countries', 'countries_df'),
          ('temperatures', 'temp_df')]


# check if the dataframes are not empty
dfs_ok = True
for table, df in tables:
    if eval(df).count() < 1:
        dfs_ok = False
        print(f'dataframe {df} is empty')

if dfs_ok: 
    print('no empties dataframes')

# check if tables as parquet files were successfully generated by the ETL
tables_ok = True
for table, df in tables:
    if not os.path.isfile(f'./data/trusted/{table}/_SUCCESS'):
        tables_ok = False
        print(f'table {table} was NOT created successfuly')

if tables_ok: 
    print('all tables were created successfully')

# check if the amount of rows between dataframes and parquet files match
rows_ok = True
for table, df in tables:
    parquet_count = spark.read.parquet(f'./data/trusted/{table}').count()
    df_count = eval(df).count()
    if parquet_count != df_count:
        rows_ok = False
        print(f'parquet files rows count does not match dataframe for table {table}')

if rows_ok:
    print('all parquet files were created with the same rows count as the respective dataframe')
    
if dfs_ok and tables_ok and rows_ok: 
    print('all quality checks passed')
```

#### 4.3 Data dictionary 
*Create a data dictionary for your data model. For each field, provide a brief description of what the data is and where it came from. You can include the data dictionary in the notebook or in a separate file.*

The data dictionary can be found [here](data-dictionary.md).

### Step 5: Complete Project Write Up
*Clearly state the rationale for the choice of tools and technologies for the project.*

Python was the language of choice since it's broadly used by data engineers and data scientists, with many libraries and the primary language used throughout this course.

PySpark was the central library of choice since it enables scalable exploratory analysis using its functions and pure SQL. It also provides excellent performance comparing to other libraries, like pandas, since it runs on top of Apache Spark. Nonetheless, it would also make the application ready to run on an EMR cluster if the amount of data increases or more performance is required.

Parquet files in the trusted zone of the datalake are also a good choice since they would easily enable the load to a database, if necessary. Also, if the database of choice is Postgres or Redshift, a single COPY command per table would be enough to transport the data. 



*Propose how often the data should be updated and why.*

The immigration data should be updated weekly, monthly, yearly, or as soon as the US National Tourism and Trade Office releases a new dataset. The same goes for the global temperature data. The other dimension tables don't need to be updated as often since the categorization data doesn't seem to change so frequently.



*Write a description of how you would approach the problem differently under the following scenarios:*
 - *The data was increased by 100x.*

In the proposed scenario, I would suggest this ETL script to run on an Apache Hadoop cluster, preferably over Amazon EMR. Also, the datasets could be stored in Amazon S3.



 - *The data populates a dashboard that must be updated on a daily basis by 7am every day.*

I'd recommend using a workflow automation tool, like Apache Airflow, to orchestrate the data pipeline and schedule the daily runs.



 - *The database needed to be accessed by 100+ people.*

I'd use Amazon Redshift as the database of choice. According to the documentation, it supports 500+ simultaneous connections, and it's also a scalable and managed service, providing a reliable tool to host the analytical tables.



-----------------

### Extra: Write a few analytics queries

A notebook with a few analytical queries can be found [here](analytics.ipynb)

-----
