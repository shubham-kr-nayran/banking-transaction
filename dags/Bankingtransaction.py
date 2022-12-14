from datetime import datetime
from airflow import DAG
import pandas as pd
from airflow.providers.amazon.aws.transfers.mysql_to_s3 import MySQLToS3Operator


with DAG(dag_id='fetch_records', description='Hello World DAG',
          schedule_interval='0 12 * * *',
          start_date=datetime(2021, 1, 1), catchup=False) as dag:

    Mysql_to_s3 = MySQLToS3Operator(
    task_id = "datatransfertos3",
    mysql_conn_id = 'bankingtd',
    aws_conn_id = 'dumpdatatos3',
    query = "select c.mname,t.transaction_amount from account as a join customer as c on a.custid=c.custid join trandetails as t on t.acnumber=a.acnumber",
    s3_bucket = 'deploymenttestbucketone',
    s3_key = 'bankingtd.csv',
    header = True,
    dag = dag 
)  
Mysql_to_s3  
