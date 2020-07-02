from jinja2 import Template
import psycopg2
import gzip
import io
import requests
import pandas as pd
import logging
import urllib.request

def check_partitions_in_table(connection, table_name):

    sql_file=open('./sql/DML/check_partitions_in_source.sql')
    template = Template(sql_file.read())
    template_sql=template.render(table_name=table_name)
    
    cursor=connection.cursor()
    cursor.execute(template_sql)
    
    return cursor.fetchall()


def stream_dataframe_to_postgres_table(connection,dataframe,table):
    
    sio = io.StringIO()
    sio.write(dataframe.to_csv(index=None, header=None, sep=","))
    sio.seek(0)

    logging.info('Inserting Dataframe into {table} table'.format(table=table))

    # Copy the string buffer to the database, as if it were an actual file
    with connection.cursor() as c:
        c.copy_from(sio, table, columns=dataframe.columns, sep=",",null='')
        connection.commit()

    logging.info('Data Set Completely inserted')