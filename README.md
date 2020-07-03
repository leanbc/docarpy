# Docarpy

Dockerized solution to run Airflow pipelines.

## Requirements:
* Docker
* Make
* AWS Credentials


## What it does:

It Extract, Transform and Loads data from the NYC yellow Taxis, starting from 2019-08-01 onwards.

## How it is done:

It creates a cluster with a postgres database and Airflow.

## If you want to connect to the Postgres Database form your local machine these are the credentials:


```
user='airflow',
password='airflow',
host='localhost',
database='airflow'
```

## Lets get started:

Open a terminal

### 1. Add AWS CREDENTIALS to the enviroment variables:

```
export AWS_ACCESS_KEY_ID=XXXXXXXXXXXXXXXXXX
export AWS_SECRET_ACCESS_KEY=XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
```

### 2. Start up the cluster:
Go to the root of the directory and run :
```
make _start_cluster
```

It will install all the depencies, may take several minutes.

### 3. Create a connection in Airflow to be able to run Postgres Operators against your database.

Once the previous step is done, you will see the Airflow logo and you will be able to access the Airlfow IU on http://localhost:8080/.
Go to another terminal and go to the root of the directory again and run.

```
make _create_postgres_connetion
```

### 4. Go the http://localhost:8080/

Turn on the `dag_taxi` and run it.

### 5. Once it is done. 

you can check the result in the postgres database.

Relevant Tables:

```
landing_nyc_tlc.stg_taxis_data
landing_nyc_tlc.taxis
landing_nyc_tlc.zone_lookup
nyc_tlc.history_table_borough_rides
nyc_tlc.history_table_zones_passengers
nyc_tlc.popular_destination_borough_rides
nyc_tlc.popular_destination_zones
```
### 6. Easy way to query??

Once data is inserted you can query from the root directory via:

```
make _get_data TABLE=popular_destination_zones PICKUP=Astoria MONTH=2019-08-01
make _get_data TABLE=popular_destination_borough_rides PICKUP=Queens MONTH=2019-08-01
```
