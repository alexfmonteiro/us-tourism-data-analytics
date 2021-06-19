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


```python
# Do all imports and installs here
import pandas as pd
import datetime as dt
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
    .getOrCreate()
```

### Step 1: Scope the Project and Gather Data

#### Scope 
Explain what you plan to do in the project in more detail:
 - Ingest 3 different datasets related to US Immigration Data, Airlines and Global Temperature
 - Explore and asses the data using this Notebook
 - Run data quality checks and exclude data that won't be used
 - Model a star schema to store the final data model 
 - Create an ETL to read the raw data, transform it and load it into a trusted zone area in a datlake.
 - The analytical tables in the trusted zone will be stored as parquet files
 - Everything will be ready to be loaded into a DW, if necessary
 - Provide a few analytical queries to validate the final tables
 - **The main purpose of the final data model is to esily provide insights about tourists and immigrants behaviours, if it's possible to correlate travels with the time of the year or with certain regions.**

What data do you use? 
 - I94 Immigration data of 2016
 - World Temperature Data
 - World Airports Data

What is your end solution look like? 
 - 8 tables stored in a star schema designed to optimize queries on immigration data in US
 - Some analytical queries to validate a few behaviours

What tools did you use?
 - Python 3.6
 - PySpark 2.4.3 using Scala version 2.11.12
 - Java HotSpot(TM) 64-Bit Server VM, 1.8.0_291
 - packages in requirements.txt
 

### Describe and Gather Data 
Describe the data sets you're using. Where did it come from? What type of information is included? 

#### I94 Immigration Data
This data comes from the US National Tourism and Trade Office. Basically, this dataset contains international visitor arrival statistics of the year 2016. 

The original dataset was in sas7bdat format, but for this project, it was transformed into parquet files since it's a smaller size and also for performance purposes when reading it into Spark dataframes.

You can read more about it [here](https://www.trade.gov/national-travel-and-tourism-office).


```python
immig_df = spark.read.parquet("./data/raw/i94-parquet")
#immig_df.limit(5).toPandas()
```

#### World Temperature Data
This dataset came from Kaggle. 

The original dataset was in CSV format, but for this project, it was transformed into parquet files since it's a smaller size and also for performance purposes when reading it into Spark dataframes.

You can read more about it [here](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data).


```python
temp_df = spark.read.parquet("./data/raw/global_temperature")
#temp_df.limit(5).toPandas()
```

-----------------------------





~~#### U.S. City Demographic Data
This dataset contains information about the demographics of all US cities and census-designated places with a population greater or equal to 65,000. This data comes from the US Census Bureau's 2015 American Community Survey.~~

~~You can read more about it [here](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/).~~


```python
# cities_df = spark.read.options(header='True', inferSchema='True', delimiter=';')\
#     .csv("./data/raw/us-cities-demographics.csv")
# cities_df.limit(5).toPandas()
```

~~#### Airport Code Table
This is a simple table of airport codes and corresponding cities. You can read more about it [here](https://datahub.io/core/airport-codes#data)~~


```python
# airport_df = spark.read.options(header='True', inferSchema='True', delimiter=',')\
#     .csv("./data/raw/airport-codes.csv")
#airport_df.limit(5).toPandas()
```

--------

#### Port of Entry Codes
This dataset contains the three-letter codes that are used by Customs and Border Protection (CBP) in its internal communications to represent ports-of-entry (POEs). It is used in the i94 immigration data in the i94port field. This data came maily from [here](https://fam.state.gov/fam/09FAM/09FAM010205.html), but also some data was merged from the file I94_SAS_Labels_Descriptions.SAS, provided by Udacity.


```python
ports_df = spark.read.options(header='True', inferSchema='True', delimiter=';')\
    .csv("./data/raw/port-of-entry-codes.csv")
#ports_df.limit(5).toPandas()
```

#### Country Codes Table
This is a simple dictionary with the codes used in I94 Forms and the corresponding country. This information was obtained from the file I94_SAS_Labels_Descriptions.SAS, provided by Udacity.


```python
countries_df = spark.read.options(header='True', inferSchema='True', delimiter=';')\
    .csv("./data/raw/country_codes.txt")
#countries_df.limit(5).toPandas()
```

#### Airline Database
This dataset came from Kaggle. It contains 5888 airline companies.

You can read more about it [here](https://www.kaggle.com/open-flights/airline-database).


```python
airlines_df = spark.read.options(header='True', inferSchema='True', delimiter=',')\
    .csv("./data/raw/airlines.csv")
#airlines_df.limit(5).toPandas()
```

### Step 2: Explore and Assess the Data
#### Explore the Data 
Identify data quality issues, like missing values, duplicate data, etc.


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
## assesssing visa data
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
Steps necessary to clean the data for each dataset:
 - drop the columns that won't be useful for the project
 - remove duplicates
 - discard invalid rows 
 - discard subsets of data outside the scope of the project
 - any additional transformation/cleaning step


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
if immig_df.distinct().count() != immig_df.count():
    immig_df.dropDuplicates()
    print('removing duplicates')
    print(f'rows count after de-duplicate: {immig_df.count()}')
else:
    print('no duplicates found')

#casting data types, relabeling column names and replacing values
immig_df.createOrReplaceTempView("immig_data")
epoch = dt.datetime(1960, 1, 1).date()
spark.udf.register("isoformat", lambda x: (epoch + dt.timedelta(x)).isoformat() if x else None)
immig_df = spark.sql('''
    SELECT int(i.cicid) id,
           i.i94port airport,
           isoformat(int(i.arrdate)) arrival_date,
           isoformat(int(i.depdate)) departure_date,   
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
           i.fltno flight_num,
           i.occup occupation,
           i.admnum admission_num,
           i.insnum,
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
    SELECT city, country, month(dt) month, avg(averagetemperature) avg_temp
      FROM temp
     WHERE lower(country) in ('united states')
       AND averagetemperature is not null
     GROUP BY city, country, month(dt)
     ORDER BY city, country, month(dt)
    ''').toPandas()
```

### Step 3: Define the Data Model
#### 3.1 Conceptual Data Model
*Map out the conceptual data model and explain why you chose that model*

The data model consists in a star schema with 1 fact table and 7 dimension tables:

 - Fact: arrivals

 - Dims: visa, 194mode, calendar, ports, airlines, countries, temperatures



#### 3.2 Mapping Out Data Pipelines
List the steps necessary to pipeline the data into the chosen data model

### Step 4: Run Pipelines to Model the Data 
#### 4.1 Create the data model
Build the data pipelines to create the data model.


```python
# Write code here
```

#### 4.2 Data Quality Checks
Explain the data quality checks you'll perform to ensure the pipeline ran as expected. These could include:
 * Integrity constraints on the relational database (e.g., unique key, data type, etc.)
 * Unit tests for the scripts to ensure they are doing the right thing
 * Source/Count checks to ensure completeness
 
Run Quality Checks


```python
# Perform quality checks here
```

#### 4.3 Data dictionary 
Create a data dictionary for your data model. For each field, provide a brief description of what the data is and where it came from. You can include the data dictionary in the notebook or in a separate file.

#### Step 5: Complete Project Write Up
* Clearly state the rationale for the choice of tools and technologies for the project.
* Propose how often the data should be updated and why.
* Write a description of how you would approach the problem differently under the following scenarios:
 * The data was increased by 100x.
 * The data populates a dashboard that must be updated on a daily basis by 7am every day.
 * The database needed to be accessed by 100+ people.


```python

```


```python

```


```python

```


```python

```


```python

```


```python

```
