import logging
import csv

from datetime import timedelta, datetime


from airflow import DAG 
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook 
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.file_to_gcs import FileToGoogleCloudStorageOperator
from airflow.providers.google.cloud.hooks.cloud_sql import CloudSQLHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

POSTGRES_CONN_ID = "postgres_localhost"
MYSQL_CONN_ID = "mysql_cloud"
GOOGLE_CONN_ID = "google_cloud_default"
PROJECT_ID="test-project-335210"
GS_PATH = "data/"
BUCKET_NAME = 'dataset_indorecipe'
TABLE_ARRAY1 = ["dataset_ayam"]
TABLE_ARRAY2 = ["dataset_ikan"]
TABLE_ARRAY3 = ["dataset_tahu"]
TABLE_ARRAY4 = ["dataset_telur"]
STAGING_DATASET = "recipe_staging_dataset"
DATASET = "recipe_dataset"
LOCATION = "asia-southeast2"


def Postgres_To_GCS1():
    for table in TABLE_ARRAY1:
        gcs_hook = GoogleCloudStorageHook(GOOGLE_CONN_ID)
        pg_hook = PostgresHook.get_hook(POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("select * from " + table)
        result = cursor.fetchall()
        with open(table +'.csv', 'w') as fp:
            a = csv.writer(fp, quoting = csv.QUOTE_MINIMAL, delimiter = ',')
            a.writerow([i[0] for i in cursor.description])
            a.writerows(result)
        logging.info("Uploading to bucket, " + table + ".csv")
        gcs_hook.upload(BUCKET_NAME, GS_PATH +table + ".csv", table + ".csv")

def Postgres_To_GCS2():
    for table in TABLE_ARRAY2:
        gcs_hook = GoogleCloudStorageHook(GOOGLE_CONN_ID)
        pg_hook = PostgresHook.get_hook(POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("select * from " + table)
        result = cursor.fetchall()
        with open(table +'.csv', 'w') as fp:
            a = csv.writer(fp, quoting = csv.QUOTE_MINIMAL, delimiter = ',')
            a.writerow([i[0] for i in cursor.description])
            a.writerows(result)
        logging.info("Uploading to bucket, " + table + ".csv")
        gcs_hook.upload(BUCKET_NAME, GS_PATH +table + ".csv", table + ".csv")

def Mysql_To_GCS1():
    for table in TABLE_ARRAY3:
        gcs_hook = GoogleCloudStorageHook(GOOGLE_CONN_ID)
        cloudsql_hook = CloudSQLHook.get_hook(MYSQL_CONN_ID)
        conn = cloudsql_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("select * from " + table)
        result = cursor.fetchall()
        with open(table +'.csv', 'w') as fp:
            a = csv.writer(fp, quoting = csv.QUOTE_MINIMAL, delimiter = ',')
            a.writerow([i[0] for i in cursor.description])
            a.writerows(result)
        logging.info("Uploading to bucket, " + table + ".csv")
        gcs_hook.upload(BUCKET_NAME, GS_PATH +table + ".csv", table + ".csv")

def Mysql_To_GCS2():
    for table in TABLE_ARRAY4:
        gcs_hook = GoogleCloudStorageHook(GOOGLE_CONN_ID)
        cloudsql_hook = CloudSQLHook.get_hook(MYSQL_CONN_ID)
        conn = cloudsql_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("select * from " + table)
        result = cursor.fetchall()
        with open(table +'.csv', 'w') as fp:
            a = csv.writer(fp, quoting = csv.QUOTE_MINIMAL, delimiter = ',')
            a.writerow([i[0] for i in cursor.description])
            a.writerows(result)
        logging.info("Uploading to bucket, " + table + ".csv")
        gcs_hook.upload(BUCKET_NAME, GS_PATH +table + ".csv", table + ".csv")

default_args = {
    'owner': 'Ilham Putra',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'start_date':  days_ago(2),
    'retry_delay': timedelta(minutes=5),
}



with DAG('RecipeWarehouse', schedule_interval='@once', default_args=default_args) as dag:
    start_pipeline = DummyOperator(
        task_id = 'start_pipeline',
        dag = dag
        )

    LocalToGCS1 = FileToGoogleCloudStorageOperator(
        task_id='LocalToGCS1',
        src='./data/recipe/dataset-tempe.csv',
        dst='data/dataset_tempe.csv',
        bucket=BUCKET_NAME,
        google_cloud_storage_conn_id='google_cloud_default',
        dag=dag
        )

    LocalToGCS2 = FileToGoogleCloudStorageOperator(
        task_id='LocalToGCS2',
        src='./data/recipe/dataset-udang.csv',
        dst='data/dataset_udang.csv',
        bucket=BUCKET_NAME,
        google_cloud_storage_conn_id='google_cloud_default',
        dag=dag
        )

    PostgresToGCS1 = PythonOperator(
        task_id="PostgresToGCS1",
        python_callable=Postgres_To_GCS1,
        )

    PostgresToGCS2 = PythonOperator(
        task_id="PostgresToGCS2",
        python_callable=Postgres_To_GCS2,
        )

    MysqlToGCS1 = PythonOperator(
        task_id="MysqlToGCS1",
        python_callable=Mysql_To_GCS1,
        )

    MysqlToGCS2 = PythonOperator(
        task_id="MysqlToGCS2",
        python_callable=Mysql_To_GCS2,
        )

    Load_staging_dataset = DummyOperator(
        task_id = 'load_staging_dataset',
        dag = dag
        )    
    
    load_dataset_ayam = GCSToBigQueryOperator(
        task_id = 'load_dataset_ayam',
        bucket = BUCKET_NAME,
        source_objects = ['data/dataset_ayam.csv'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.dataset_ayam',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        schema_fields=[
        {'name': 'Title', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'Ingredients', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Steps', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Loves', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'URL', 'type': 'STRING', 'mode': 'NULLABLE'},
            ]
        )

    load_dataset_ikan = GCSToBigQueryOperator(
        task_id = 'load_dataset_ikan',
        bucket = BUCKET_NAME,
        source_objects = ['data/dataset_ikan.csv'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.dataset_ikan',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        schema_fields=[
        {'name': 'Title', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'Ingredients', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Steps', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Loves', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'URL', 'type': 'STRING', 'mode': 'NULLABLE'},
            ]
        )

    load_dataset_tahu = GCSToBigQueryOperator(
        task_id = 'load_dataset_tahu',
        bucket = BUCKET_NAME,
        source_objects = ['data/dataset_tahu.csv'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.dataset_tahu',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        schema_fields=[
        {'name': 'Title', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'Ingredients', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Steps', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Loves', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'URL', 'type': 'STRING', 'mode': 'NULLABLE'},
            ]
        )
    
    load_dataset_telur = GCSToBigQueryOperator(
        task_id = 'load_dataset_telur',
        bucket = BUCKET_NAME,
        source_objects = ['data/dataset_telur.csv'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.dataset_telur',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        schema_fields=[
        {'name': 'Title', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'Ingredients', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Steps', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Loves', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'URL', 'type': 'STRING', 'mode': 'NULLABLE'},
            ]
        )   

    load_dataset_tempe = GCSToBigQueryOperator(
        task_id = 'load_dataset_tempe',
        bucket = BUCKET_NAME,
        source_objects = ['data/dataset_tempe.csv'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.dataset_tempe',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        schema_fields=[
        {'name': 'Title', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'Ingredients', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Steps', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Loves', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'URL', 'type': 'STRING', 'mode': 'NULLABLE'},
            ]
        )

    load_dataset_udang = GCSToBigQueryOperator(
        task_id = 'load_dataset_udang',
        bucket = BUCKET_NAME,
        source_objects = ['data/dataset_udang.csv'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.dataset_udang',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        schema_fields=[
        {'name': 'Title', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'Ingredients', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Steps', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Loves', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'URL', 'type': 'STRING', 'mode': 'NULLABLE'},
            ]
        )
    
    check_dataset_ayam = BigQueryCheckOperator(
        task_id = 'check_dataset_ayam',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{STAGING_DATASET}.dataset_ayam`'
        )

    check_dataset_ikan = BigQueryCheckOperator(
        task_id = 'check_dataset_ikan',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{STAGING_DATASET}.dataset_ikan`'
        )

    check_dataset_tahu = BigQueryCheckOperator(
        task_id = 'check_dataset_tahu',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{STAGING_DATASET}.dataset_tahu`'
        ) 

    check_dataset_telur = BigQueryCheckOperator(
        task_id = 'check_dataset_telur',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{STAGING_DATASET}.dataset_telur`'
        )

    check_dataset_tempe = BigQueryCheckOperator(
        task_id = 'check_dataset_tempe',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{STAGING_DATASET}.dataset_tempe`'
        )               

    check_dataset_udang = BigQueryCheckOperator(
        task_id = 'check_dataset_udang',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{STAGING_DATASET}.dataset_udang`'
        ) 

    Create_D_Table = DummyOperator(
        task_id = 'Create_D_Table',
        dag = dag
        )

    create_D_dataset_ayam = BigQueryOperator(
        task_id = 'create_D_dataset_ayam',
        use_legacy_sql = False,
        location = LOCATION,
        sql = './sql/D_dataset_ayam.sql'
        )

    create_D_dataset_ikan = BigQueryOperator(
        task_id = 'create_D_dataset_ikan',
        use_legacy_sql = False,
        location = LOCATION,
        sql = './sql/D_dataset_ikan.sql'
        )   

    create_D_dataset_tahu = BigQueryOperator(
        task_id = 'create_D_dataset_tahu',
        use_legacy_sql = False,
        location = LOCATION,
        sql = './sql/D_dataset_tahu.sql'
        )

    create_D_dataset_telur = BigQueryOperator(
        task_id = 'create_D_dataset_telur',
        use_legacy_sql = False,
        location = LOCATION,
        sql = './sql/D_dataset_telur.sql'
        )

    create_D_dataset_tempe = BigQueryOperator(
        task_id = 'create_D_dataset_tempe',
        use_legacy_sql = False,
        location = LOCATION,
        sql = './sql/D_dataset_tempe.sql'
        )         

    create_D_dataset_udang = BigQueryOperator(
        task_id = 'create_D_dataset_udang',
        use_legacy_sql = False,
        location = LOCATION,
        sql = './sql/D_dataset_udang.sql'
        )

    create_F_dataset_recipe = BigQueryOperator(
        task_id = 'create_F_dataset_recipe',
        use_legacy_sql = False,
        location = LOCATION,
        sql = './sql/F_dataset_recipe.sql'
        )

    check_F_dataset_recipe = BigQueryCheckOperator(
        task_id = 'check_F_dataset_recipe',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{DATASET}.F_dataset_recipe`'
        ) 

    finish_pipeline = DummyOperator(
        task_id = 'finish_pipeline',
        dag = dag
        )

start_pipeline >> [LocalToGCS1, LocalToGCS2, PostgresToGCS1, PostgresToGCS2, MysqlToGCS1, MysqlToGCS2] >> Load_staging_dataset

Load_staging_dataset >> [load_dataset_ayam, load_dataset_ikan, load_dataset_tahu, load_dataset_telur, load_dataset_tempe, load_dataset_udang]

load_dataset_ayam >> check_dataset_ayam
load_dataset_ikan >> check_dataset_ikan
load_dataset_tahu >> check_dataset_tahu
load_dataset_telur >> check_dataset_telur
load_dataset_tempe >> check_dataset_tempe
load_dataset_udang >> check_dataset_udang

[check_dataset_ayam, check_dataset_ikan, check_dataset_tahu, check_dataset_telur, check_dataset_tempe, check_dataset_udang] >> Create_D_Table

Create_D_Table >> [create_D_dataset_ayam, create_D_dataset_ikan, create_D_dataset_tahu, create_D_dataset_telur, create_D_dataset_tempe, create_D_dataset_udang]

[create_D_dataset_ayam, create_D_dataset_ikan, create_D_dataset_tahu, create_D_dataset_telur, create_D_dataset_tempe, create_D_dataset_udang] >> create_F_dataset_recipe

create_F_dataset_recipe >> check_F_dataset_recipe >> finish_pipeline
