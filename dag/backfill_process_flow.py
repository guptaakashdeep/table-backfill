from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from datetime import date, datetime, time, timedelta
import boto3
import json
import botocore
import json, yaml, random


#Create bucket and key by splitting filename
def path_to_bucket_key(path):   
    if not path.startswith('s3://'):
        raise ValueError('S3 path provided is incorrect: ' + path)
    path = path[5:].split('/')
    bucket = path[0]
    key = '/'.join(path[1:])
    return bucket, key


#Read the S3 bucket file and reeturn data as string
def read_s3_file(filepath, encoding='utf8'): 
    client = boto3.client('s3')
    bucket, key = path_to_bucket_key(filepath)
    obj = client.get_object(Bucket=bucket, Key=key)
    return obj['Body'].read().decode(encoding)


#Extract the AWS account id
with open('/usr/local/airflow/ssh/variables.json') as json_file:
    data = json.load(json_file) 
aws_account_id = data['AccountId']
json_file.close()

#Config file attributes
backfill_yaml = f"s3://bucket-eu-west-1-{aws_account_id}-code/dags/config/rtd2_backfill_config.yaml"
BACKFILL_CONTENT = read_s3_file(backfill_yaml)
BACKFILL_DICT = yaml.safe_load(BACKFILL_CONTENT)
KEY_FILE = "/usr/local/ec2-emr-key.pem"
IP = open("/usr/local/emr-ip.txt","r").read()
emr_rtd_path="/home/hadoop/utilities"
script_path = f"{emr_rtd_path}/table_backfill"
queue="airflow-sql-queue"

#Defining the default arguments of DAG
default_args = {
    'owner': 'frosty',
    'depends_on_past': False,
    'email': ['geekfrosty@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'queue': queue,
    'retries': 0,
    'end_date': datetime(2099, 12, 31)
}

#Define emr logon command
ssh_cmd = f"ssh -o StrictHostKeyChecking=no -t -i {KEY_FILE} hadoop@{IP} "
backfill_tables = BACKFILL_DICT['tables']

with DAG(
    dag_id="rtd_backfill_process",
    start_date=datetime(2023, 11, 3),
    max_active_runs=1,
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
) as dag:
    # Operator definition
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    backup_start = EmptyOperator(task_id='backup_start')
    backup_end = EmptyOperator(task_id='backup_end')
    backfill_start = EmptyOperator(task_id='backfill_start')
    backfill_end = EmptyOperator(task_id='backfill_end')
    # TODO: change this to Branch operator to choose between backup and backfill
    choose_process = EmptyOperator(task_id="choose_process")

    process_list = []

    trigger_cmd = f"""{ssh_cmd} spark-submit -v --master=yarn --driver-memory=19g \
                                                --conf spark.driver.cores=5 \
                                                --conf spark.driver.memoryOverhead=2g \
                                                --conf spark.dynamicAllocation.maxExecutors=16 \
                                                --conf spark.default.parallelism=200  --conf spark.sql.shuffle.partitions=200 \
                                                --conf spark.dynamicAllocation.executorIdleTimeout=300 \
                                                --conf spark.sql.broadcastTimeout=3600 \
                                                {script_path}/backfill_tables.py"""

    for table in backfill_tables:
        _,tname = table.split(".")
        backfill_trigger_script = BashOperator(
            task_id=f"create_backfill_{tname}",
            bash_command=(f"{trigger_cmd} -t {table} --backfill")
        )
        populate_table = BashOperator(
            task_id=f"populate_{tname}",
            bash_command=(f"{trigger_cmd} -t {table}")
        )
        start.set_downstream(backfill_trigger_script)
        backfill_trigger_script.set_downstream(populate_table)
        end.set_upstream(populate_table)
