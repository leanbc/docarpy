import boto3
import logging
import pandas as pd
import psycopg2
from time import sleep
from datetime import datetime
import json
import os
from io import StringIO
from jinja2 import Template
from helpers.sql import check_partitions_in_table
from helpers.sql import stream_dataframe_to_postgres_table
import urllib.request
import sys

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)


def dim_table():


    sleep(10)
    connection=psycopg2.connect(user='airflow',
                            password='airflow',
                            host='postgres',
                            database='airflow')

    connection.autocommit = True
    cursor = connection.cursor()
    # Print PostgreSQL Connection properties
    logging.info('connecetion parameters: {parameters}'.format(parameters=connection.get_dsn_parameters()))


    bucket_name='nyc-tlc'
    file_name='taxi+_zone_lookup'
    url='https://s3.amazonaws.com/{bucket_name}/misc/{file_name}.csv'.format(bucket_name=bucket_name,file_name=file_name.replace(' ','+'))
    logging.info('Downloading Data')
    urllib.request.urlretrieve(url,'./data/'+ file_name.replace(' ','_'))
    logging.info('Data Downloaded')

    #Creating final and landing Schema 
    sqlfile = open('./sql/DDL/create_schema.sql')
    template = Template(sqlfile.read())
    template_sql=template.render(schema_name=bucket_name.replace('-','_'))
    cursor.execute(template_sql)
    logging.info('Schema {bucket_name} Created'.format(bucket_name=bucket_name.replace('-','_'))) 

    sqlfile = open('./sql/DDL/create_schema.sql')
    template = Template(sqlfile.read())
    template_sql=template.render(schema_name='landing_'+ bucket_name.replace('-','_'))
    cursor.execute(template_sql)
    logging.info('Schema {bucket_name} Created'.format(bucket_name='landing_'+ bucket_name.replace('-','_'))) 

    partititon_in_source=False
    try:
        list_of_partitions=check_partitions_in_table(connection,'landing_' + bucket_name.replace('-','_') + '.zone_lookup')
        for partititon in list_of_partitions:
            if partititon[0]==file_name:
                logging.info('Partition already inserted.')
                partititon_in_source=True
                continue
    except:
        logging.info('Table does not exist and will be created')
        df=pd.read_csv('./data/'+ file_name.replace(' ','_'),nrows=500)
        df['bucket']=bucket_name
        df['file_name']=file_name
        create_statement=pd.io.sql.get_schema(df,'landing_' + bucket_name.replace('-','_') + '.zone_lookup')
        create_statement=create_statement.replace('"','')
        create_statement=create_statement.replace('CREATE TABLE','CREATE TABLE IF NOT EXISTS')
        cursor.execute(create_statement)

    for chunk in pd.read_csv('./data/'+ file_name.replace(' ','_'), chunksize=100000,sep=',',dtype='unicode' ):
        if partititon_in_source==False:
            logging.info('Chunking up DataFrame')
            df=chunk
            print(df.dtypes)
            df['bucket']=bucket_name
            df['file_name']=file_name
            #Insert data
            stream_dataframe_to_postgres_table(connection=connection,dataframe=df,table='landing_' + bucket_name.replace('-','_') + '.zone_lookup')
        else:
            logging.info('Data already in the table. Insert no need')

def main():
    
    sleep(10)
    connection=psycopg2.connect(user='airflow',
                            password='airflow',
                            host='postgres',
                            database='airflow')

    connection.autocommit = True
    cursor = connection.cursor()
    # Print PostgreSQL Connection properties
    logging.info('connecetion parameters: {parameters}'.format(parameters=connection.get_dsn_parameters()))


    #Creating S3 Client
    client=boto3.client('s3')

    bucket_name='nyc-tlc'
    prefix='trip data'

    response = client.list_objects_v2(
        Bucket=bucket_name,
        Prefix=prefix,
    )


    #listing all files in the bucket
    files=[content['Key'] for content in response['Contents'] if content['Key'].endswith(".csv")]
    logging.info('list of files in the Bucket')
    logging.info(files)

    #Getting only yellow_trips
    yellow_2019=[file_name for file_name in files  if 'yellow' in file_name and '2019' in file_name ]
    logging.info('Getting only 2019 Yellow trips')
    logging.info(yellow_2019)

    #Getting last 3 files
    logging.info('looping over the last 3 months available:') 
    logging.info(yellow_2019[-3:])


    for file_name in yellow_2019[-5:]:

        logging.info('Trying to insert file_name:{file_name}'.format(file_name=file_name))


        partititon_in_source=False
        #checking partition in source
        try:
            list_of_partitions=check_partitions_in_table(connection,'landing_' + bucket_name.replace('-','_') + '.taxis')
            for partititon in list_of_partitions:
                if partititon[0]==file_name:
                    logging.info('Partition already inserted.')
                    partititon_in_source=True
        except:
            logging.info('Table does not exist and will be created')


        if partititon_in_source:
            logging.info('Partition already inserted, skipping to next iteration ') 
            continue

        logging.info('Getting file {file_name}'.format(file_name=file_name))

        url='https://{bucket_name}.s3.amazonaws.com/{file_name}'.format(bucket_name=bucket_name,file_name=file_name.replace(' ','+'))


        logging.info('Downloading Data')

        urllib.request.urlretrieve(url,'./data/'+ file_name.replace(' ','_'))

        logging.info('Data Downloaded')
        
        #create table if not exists
        df=pd.read_csv('./data/'+ file_name.replace(' ','_'),nrows=500)
        df['bucket']=bucket_name
        df['file_name']=file_name
        create_statement=pd.io.sql.get_schema(df,'landing_' + bucket_name.replace('-','_') + '.taxis')
        create_statement=create_statement.replace('"','')
        create_statement=create_statement.replace('CREATE TABLE','CREATE TABLE IF NOT EXISTS')
        cursor.execute(create_statement)
        logging.info('Creating Table If not exists')

        file_completely_inserted=False
        i=1
        for chunk in pd.read_csv('./data/'+ file_name.replace(' ','_'), chunksize=100000,sep=',',dtype='unicode' ):
            logging.info('Chunking up DataFrame')
            df=chunk
            print(df.dtypes)
            df['bucket']=bucket_name
            df['file_name']=file_name
            #Insert data
            logging.info('Inserting Chunk in Postgres number {i}'.format(i=i))
            stream_dataframe_to_postgres_table(connection=connection,dataframe=df,table='landing_' + bucket_name.replace('-','_') + '.taxis')
            i+=1
            file_completely_inserted=True
        
        if file_completely_inserted:
            logging.info('Stopping pipeline since one file has been already inserted')
            break
    
    os.remove('./data/'+ file_name.replace(' ','_'))
    logging.info('File {file} has been removed'.format(file=file_name.replace(' ','_')))

def run_sql(file_path):

    sleep(10)
    connection=psycopg2.connect(user='airflow',
                            password='airflow',
                            host='postgres',
                            database='airflow')

    connection.autocommit = True
    cursor = connection.cursor()
    # Print PostgreSQL Connection properties
    logging.info('connecetion parameters: {parameters}'.format(parameters=connection.get_dsn_parameters()))
    
    sqlfile = open(file_path, 'r')
    query=sqlfile.read()
    logging.info('Executing {file_path}'.format(file_path=file_path))
    logging.info(query)
    cursor.execute(query)



