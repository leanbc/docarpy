import pprint
import psycopg2
import pandas as pd
import logging
import argparse

def get_data(table,pickup,month):
    
    connection=psycopg2.connect(user='airflow',
                        password='airflow',
                        host='postgres',
                        port=5432,
                        database='airflow')

    
    pp = pprint.PrettyPrinter(indent=4)
    connection.autocommit = True
    cursor = connection.cursor()

    
    if table=='popular_destination_borough_rides':
        query='''
        SELECT
            month::date
            ,pickup_borough
            ,dropoff_borough
            ,rank_sum_passenger_count
        FROM
            nyc_tlc.popular_destination_borough_rides
        Where
            pickup_borough='{pickup}'
            AND month='{month}'
        Order by month,rank_sum_passenger_count
        '''.format(pickup=pickup,month=month)
    elif table=='popular_destination_zones':
        query='''
        SELECT
            month::date
            ,pickup_zone
            ,dropoff_zone
            ,rank_sum_passenger_count
        FROM
            nyc_tlc.popular_destination_zones
        Where
            pickup_zone='{pickup}'
            AND month='{month}'
        Order by month,rank_sum_passenger_count
        '''.format(pickup=pickup,month=month)
        

    cursor.execute(query)
    
    mapped_result=list(map(lambda x: [x[0].strftime('%Y-%m-%d'),x[1],x[2],x[3]],cursor.fetchall()))
    
    cols=[i[0] for i in cursor.description]
    
    df=pd.DataFrame(mapped_result,columns=cols)
     
    print(df)
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--table')
    parser.add_argument('--pickup')
    parser.add_argument('--month')
    args = parser.parse_args()


    table=args.table
    pickup=args.pickup
    month=args.month   

    get_data(table,pickup,month)