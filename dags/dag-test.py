from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash import BashOperator

from pprint import pprint
import pandas as pd


import os

from datetime import datetime
import subprocess

table_query = """CREATE TABLE vehicles (
                    id TEXT,
                    vin TEXT,
                    price TEXT,
                    miles TEXT, 
                    stock_no TEXT, 
                    year TEXT, 
                    make TEXT, 
                    model TEXT, 
                    trim TEXT, 
                    body_type TEXT, 
                    vehicle_type TEXT, 
                    drivetrain TEXT, 
                    transmission TEXT, 
                    fuel_type TEXT, 
                    engine_size TEXT, 
                    engine_block TEXT,
                    seller_name TEXT, 
                    street TEXT, 
                    city TEXT, 
                    state TEXT, 
                    zip  TEXT
                    );      
                """


def _insert_data():

    post_hook = PostgresHook(postgres_conn_id = 'postgres')
    post_hook.copy_expert("COPY vehicles FROM STDIN WITH DELIMITER ',' CSV HEADER ",'files/vehicle_challenge.csv')

    

def _preprocess_data():

    print("To avoid adding sh files to the container I added some subprocess to mimic a shell script")

    print('Line Count: ', subprocess.getoutput('wc -l files/vehicle_challenge.csv  '))

    print('Columns Count: ', subprocess.getoutput("awk -F, '{print NF; exit}' files/vehicle_challenge.csv"))

    print('Headers: ', subprocess.getoutput('head -1 files/vehicle_challenge.csv') )

    print("This is a python solution")

    df = pd.read_csv('files/vehicle_challenge.csv')
    print(f'The file contains: Lines {df.shape[0]+1} from it contains {df.shape[0]} rows and  {df.shape[1]} columns')
    print(f'The file headers: {", ".join(df.columns.to_list())}')


def _general_statistics():
    post_hook = PostgresHook(postgres_conn_id = 'postgres')

    results = post_hook.get_records('SELECT * FROM vehicles limit 10')   

    print(results)

    results = post_hook.get_records('SELECT count(*) from vehicles;')   

    print(f'TOTAL RECORDS: {results[0][0]}')

    results = post_hook.get_records('SELECT count(distinct vin)  from vehicles;')   

    print(f'TOTAL unique VINS: {results[0][0]}')

    results = post_hook.get_records('SELECT count(*) from vehicles where vin is not null and price is not null;')   

    print(f'TOTAL unique VINS: {results[0][0]}')
# # ,NULLIF(NULLIF(zip, 'J0J 1J0'),'')::integer
#  , miles::integer
                                        # , year::integer
                                        # , NULLIF(engine_size, '')::DOUBLE PRECISION
    post_hook.run("DELETE FROM vehicles WHERE price = 'price' ;")   
                                        
    results = post_hook.get_pandas_df("""SELECT  
                                        NULLIF(price,'')::integer
                                        ,  NULLIF(miles,'')::integer
                                        , NULLIF(year, '')::integer
                                        , NULLIF(engine_size, '')::DOUBLE PRECISION
                                        ,NULLIF(NULLIF(NULLIF(zip, 'J0J 1J0'),'V1N 1H9'),'')::integer
                                        
                                        FROM vehicles;""")   
    
    print(results.shape)
    print('SHAPES PRE ALTER TABLE:' ,post_hook.get_pandas_df('select * from vehicles' ).shape)

    post_hook.run("""
                ALTER TABLE vehicles
                ALTER COLUMN price TYPE INTEGER USING NULLIF(price,'')::integer
                ,ALTER COLUMN miles TYPE INTEGER USING NULLIF(miles,'')::integer
                ,ALTER COLUMN year TYPE INTEGER USING NULLIF(year,'')::integer
                ,ALTER COLUMN engine_size TYPE DOUBLE PRECISION USING NULLIF(engine_size, '')::DOUBLE PRECISION
                ,ALTER COLUMN zip TYPE INTEGER USING NULLIF(NULLIF(NULLIF(zip, 'J0J 1J0'),'V1N 1H9'),'')::integer
                ;
    """)

    print('SHAPES POST ALTER TABLE:' ,post_hook.get_pandas_df('select * from vehicles' ).shape)

    results = post_hook.get_pandas_df("""
                                    SELECT 
                                    count(*) FILTER (WHERE price is NULL) AS price_lost_records,
                                    count(*) FILTER (WHERE miles is NULL) AS miles_lost_records,
                                    count(*) FILTER (WHERE year is NULL) AS year_lost_records,
                                    count(*) FILTER (WHERE engine_size is NULL) AS engine_size_lost_records,
                                    count(*) FILTER (WHERE zip is NULL) AS zip_lost_records
                                    FROM vehicles;
                                    """)   
    
    pprint(results.T)

    print('SHAPES BEFORE  ALTER TABLE:' ,post_hook.get_pandas_df('select * from vehicles' ).shape)

    results = post_hook.get_pandas_df("""
                                    SELECT city,count(*) as count from vehicles 
                                    where vehicle_type = 'Truck' and model in ('F-150','F-150 Heritage') 
                                    group by city
                                    order by count desc
                                    limit 10
                                    ;
                                    """)   
    
    print(results.head(10))


types = {
    'gasoline' : ["E85 / Unleaded",'Unleaded','Premium Unleaded',
                'Premium Unleaded; Unleaded','Premium Unleaded / Unleaded',
                'Unleaded; Unleaded / E85','Unleaded / E85','E85 / Premium Unleaded',
                'Premium Unleaded; Premium Unleaded / E85','Premium Unleaded / Unleaded; Unleaded'],
    'hybrid' : ['Electric / Premium Unleaded','Unleaded / Electric','Electric / Unleaded',
                'Premium Unleaded / Natural Gas','Unleaded; Unleaded / Natural Gas'
                'Compressed Natural Gas; Unleaded','Electric / Premium Unleaded; Electric / Unleaded'],
    'diesel' : ['Diesel','Diesel; Unleaded'],
    'gas':['Compressed Natural Gas'],
    'electric':['Electric','Electric / Hydrogen']

}

def map_fuels(x,types):
    for full_type in types:
        if x in types[full_type]:
            return full_type



def _remove_duplicates ():
    post_hook = PostgresHook(postgres_conn_id = 'postgres')

    query = """
            with no_dups as (
            select * 
            ,row_number() over(PARTITION by vin order by miles desc, miles desc) as row_number
            ,rank() over(PARTITION by vin order by miles desc) as rank
            from vehicles
            )
            select * from no_dups where row_number =1
    """
    post_hook.run(query)

def _transformations():

    post_hook = PostgresHook(postgres_conn_id = 'postgres')

    add_miles_range_query = """ALTER TABLE vehicles ADD COLUMN miles_range  VARCHAR;"""
    post_hook.run(add_miles_range_query)

    update_miles_range_query = """UPDATE vehicles SET miles_range = CASE WHEN miles = 0 THEN 'new' WHEN miles >0 and miles <=3000  THEN 'semi-new' ELSE 'used' END;"""
    post_hook.run(update_miles_range_query)

    results = post_hook.get_pandas_df("select miles_range,count(*) from vehicles group by miles_range")
    print(results.T) 

    new_fuel_query = """
                ALTER TABLE vehicles ADD COLUMN new_fuel_type  VARCHAR;

                UPDATE vehicles 
                SET new_fuel_type =
                CASE WHEN fuel_type in ('E85 / Unleaded','Unleaded','Premium Unleaded',
                'Premium Unleaded; Unleaded','Premium Unleaded / Unleaded',
                'Unleaded; Unleaded / E85','Unleaded / E85','E85 / Premium Unleaded',
                'Premium Unleaded; Premium Unleaded / E85','Premium Unleaded / Unleaded; Unleaded') THEN 'gasoline'
				WHEN fuel_type in ('Electric / Premium Unleaded','Unleaded / Electric','Electric / Unleaded',
                'Premium Unleaded / Natural Gas','Unleaded; Unleaded / Natural Gas'
                'Compressed Natural Gas; Unleaded','Electric / Premium Unleaded; Electric / Unleaded') THEN 'hybrid'
				WHEN fuel_type in ('Diesel','Diesel; Unleaded') THEN 'diesel'
				WHEN fuel_type in ('Compressed Natural Gas') THEN 'gas'
				WHEN fuel_type in ('Electric','Electric / Hydrogen') then 'electric'
				ELSE NULL END;
                """

    post_hook.run(new_fuel_query)

    average_miles_query = """
                        ALTER TABLE vehicles
                        ADD COLUMN average_miles  FLOAT;
                        UPDATE vehicles t1
                        SET average_miles = t2.average_miles
                        FROM 
                        (select make,model,year, round(avg(miles),2) as average_miles from vehicles group by make,model,year) t2
                        WHERE t1.make = t2.make
                        AND t1.model = t2.model
                        AND t1.year = t2.year;
    
                        """

    post_hook.run(average_miles_query)

    sellers_table_query = """
                            DROP TABLE IF EXISTS  vehicles_sellers;
                            CREATE  TABLE vehicles_sellers AS 

                            with sellers_vehicles as (
                            select seller_name,street,vin from vehicles group by 1,2,3 
                            )

                            select seller_name,street, count(*) as amount_vehicles from sellers_vehicles
                            group by 1,2 
                            HAVING count(*)>=5;
    """


    post_hook.run(sellers_table_query)

    


with DAG('ltv-pipeline', start_date = datetime(2022,8,2),schedule_interval = None,catchup=False) as dag:

    drop_table = PostgresOperator(
        task_id = 'drop_table',
        postgres_conn_id = 'postgres',
        sql = """DROP TABLE IF EXISTS vehicles"""
    )

    create_table = PostgresOperator(
        task_id = 'create_table',
        postgres_conn_id = 'postgres',
        sql = table_query
    )

    decompress_file = BashOperator(
        task_id = 'decompress-file',
        bash_command='if [ -f *vehicle_challenge.csv.gz ]; then  gunzip -f /opt/airflow/files/vehicle_challenge.csv.gz; fi;'
    )

    pre_process = PythonOperator(
        task_id = 'pre-process',
        python_callable = _preprocess_data
    )
    
    insert_data = PythonOperator(
        task_id = 'load-data',
        python_callable = _insert_data
    )

    general_statistics = PythonOperator(
        task_id = 'general-statistics',
        python_callable = _general_statistics

    )
    remove_duplicates = PythonOperator(
        task_id = 'remove-duplicates',
        python_callable = _remove_duplicates

    )
    transformations = PythonOperator(
        task_id = 'transformation',
        python_callable = _transformations

    )

drop_table >> decompress_file >> pre_process >> create_table >> insert_data >> general_statistics >> remove_duplicates >> transformations