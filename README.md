# brazil-government-expenses
Build a datalakehouse analyze brazilian government direct expenses and credit card usage

## Project Description

In this project, a ETL process is created to process Brazil's government historical expenses, from both direct expenses
such as government investments, acts or other type of official expenses and public agents expenses using corporate cards.

Since 2014 a law in Brazil obligates government to provide access to all their data. To provide this data, the "Portal da Transparencia" was created.
This project reads data from two "Portal da Transparencia" sources.

The processing is done using pyspark and everything is orchestrated by Airflow. In a monthly DAG that transforms 
the data in a structured datawarehouse in the bucket, generating parquet tables.  

Card data is retrieved directly from the Portal da Transparencia API, and raw data is saved to the bucket, in Json format.

Direct expenses must be downloaded and unzipped from http://transparencia.gov.br/download-de-dados/despesas
in CSV format.   

Processed data from 2017 to 2019 has been processed and are available in dimension and fact directories.

#### The below S3 bucket contans the data processed in the project.

https://s3.console.aws.amazon.com/s3/buckets/brgovdata

Raw data from 2017 to 2020 is available in the raw_data/ folder.
Also normalized data from 2017 to 2019 is available in the dimension/ and fact/ folders.

### Bucket Structure
*bucket*/raw_data/despesas/ - CSV files downloaded from Brazil Government Portal da Transparencia

*bucket*/raw_data/credit_card_vouchers - Vouchers from corporate credit card, extracted from API in task "Extract_Card_Vouchers"

*bucket*/staging/ - process generate "temp" files needed for table calculations

*buket*/dimension/ - dimension tables are stored in this directory

*bucket*/fact/ - fact tables are stored in this directory.

### Data Cleansing
Date fields, stored as string in the files needed to be parsed, so they can be stored as Date format.

Value fields, needed parsing, as in Brazil's locale, comma is used as decimal separator and period as thousand separator.
So periods needed to be removed from string and then comma transformed to period so the number could be cast as float.

### Source Data (Considering Data from 2017 to 2019)
#### Direct Expenses 
Despesas_Pagamento_YYYYMMDD.csv - 28M rows

Despesas_Empenho_YYYYMMDD.csv - 10M rows

#### Credit Card Vouchers
corporate-card-expenses_YYYY-MM-DD - 1M rows

### Analytics Schema
Three dimension tables were extracted from data:

agency, city, vendor

Agency and vendor tables are obtained from both sources,
and joined, to enrich the output data. 

## Process Steps
1. Read data from API, writing json files to bucket.
    
2. Stage data from raw_files, in this step duplicates are removed, 
data with null keys are removed, the process checks if 
data is already in the dimension tables and generates the staging tables with delta in data.

3. Append staged records to dimension table, generating a autoincrement column id

4. Process fact tables, retrieving ids from dimension tables and dropping non fact columns.

5. Data validation

   5.1. Duplicate Keys in Dimension Tables
   5.2. Percentage of records in fact tables without reference to any dimension table
   
#### Data Modeling

![Diagram](https://github.com/marcel-luchi/brazil-government-expenses/blob/master/diagram.png)   

#### Future data updates
Process is already prepared to append new data.
Dimension tables work as append.
Fact tables are partitioned by year/month and process overwrites the partition being processed
so new data will be added with no problems and data will be overwritten if a month is reprocessed.


### Installation

Place airflow objects under your airflow directory

AWS utils has functions to create a redshift cluster, and arn roles.
After cluster is created, run *create_tables.sql* in order to create
all needed tables in redshift.

Create the below connections in Airflow:
**_aws_credentials_**, type aws with key and secret to aws S3

Create the below variables in Aiflow:

**_transparencia_access_key_** create a key in http://portaldatransparencia.gov.br/api-de-dados/cadastrar-email
or use value 0c21962b63882c5b1e5e148862305bb2

**_transparencia_bucket_** data Bucket or local root directory 


## Usage

Run **Extract_Government_Data** in airflow

task read_vouchers_operator in read_card_data.py has default sleep time of 1 second between
requests, this is due to API restriction, that limits each user to place a maximum of 30 requests
per minute. As each request responds in just over one second, this will prevent the API key from 
being blocked.


##Scenario Questions
*How you would approach the problem differently under the following scenarios:*

**If the data was increased by 100x.**

As the process gathers data based on a monthly basis, if the total amount of data increases bu the monthly data does not, there will be no problems.
If monthly data increases by 100x, we might want to spin up a more powerful EMR cluster in order to handle a 100x bigger dataset (increase the number of cores and perhaps consider an optimized EMR cluster, depending on if we care more about storage or more about computing power).

**If the pipelines were run on a daily basis by 7am.**

Only airflow schedule will need to be changed.

The process gathers data from all the execution_date month and overwrites the month partition in fact tables.

For dimension tables, as the process only appends new data, no changes in code are needed too.

