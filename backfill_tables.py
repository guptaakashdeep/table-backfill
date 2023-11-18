from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, when
from copy import deepcopy
import boto3
from botocore.exceptions import ClientError
import argparse
from datetime import datetime
import json
import pandas as pd


spark = SparkSession.builder.master("yarn").enableHiveSupport().getOrCreate()
spark.conf.set("spark.sql.crossJoin.enabled", "true")
spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
spark.conf.set("hive.exec.dynamic.partition", "true")


# define boto3 clients globally
REGION = 'eu-west-1'
glue_client = boto3.client('glue', region_name=REGION)
s3_client = boto3.client('s3', region_name=REGION)


def _get_table(db_name, table_name):
    try:
        response = glue_client.get_table(DatabaseName=db_name,
                         Name=table_name)
        return response
    except ClientError as error:
            err_response = error.response
            if err_response["Error"]["Code"] == "EntityNotFoundException":
                print(f"{err_response['Message']} => {db_name}.{table_name}")
            raise error


def _create_table_request(table_detail, backup_prefix):
    new_table = deepcopy(table_detail.get('Table'))
    # update location in sotrage descriptor
    location = new_table.get('StorageDescriptor').get('Location')
    new_location = f'{location}{backup_prefix}'
    new_storage_descriptor = new_table.get('StorageDescriptor')
    new_storage_descriptor['Location'] = new_location
    new_storage_descriptor['SerdeInfo']['Parameters'].pop('path', None)
    partition_keys = new_table.get('PartitionKeys')
    backup_table_name = f"{new_table.get('Name')}{backup_prefix}"
    request = {
        'Name': backup_table_name,
        'StorageDescriptor': new_storage_descriptor,
        'PartitionKeys': partition_keys,
        'TableType': 'EXTERNAL_TABLE'
    }
    return request


def _get_bucket_key(s3_path):
    """
    Gets the S3 bucket and key.
    :param s3_path: str
    :return: tuple
    """
    s3_path = s3_path.replace("s3://", "")
    s3_path_list = s3_path.split("/", 1)
    if len(s3_path_list) < 2:
        raise Exception("Incorrect S3 path.")
    s3_bucket = s3_path_list[0]
    s3_key = s3_path_list[1]
    return s3_bucket, s3_key


def _sync_data_type(spark, src_df, tgt_tbl):
    """
    Syncs the data type between the dataframe and target table where data will be written.
    :param spark: Spark Session
    :param src_df: pyspark dataframe
    :param tgt_tbl: str
    :return: pyspark dataframe
    """
    df_schema_fields = json.loads(src_df.schema.json())['fields']
    # Reading target table:
    tgt_df = spark.read.table(tgt_tbl).limit(1)
    tgt_schema_fields = json.loads(tgt_df.schema.json())['fields']
    for mtdt in tgt_schema_fields:
        src_df = src_df.withColumn(mtdt['name'], col(mtdt['name']).cast(mtdt['type']))
    return src_df


def _update_df_schema(spark, df, tbl_name):
    """
    generates final spark dataframe with proper column sequence of output table as mentioned

    params :
        spark : spark connection
        df : Input DataFrame
        tbl_name : Name of the output table / output DF

    return :
        final_df : Final proper sequenced Dataframe

     """
    from pyspark.sql import DataFrame
    import json

    if not isinstance(df, DataFrame):
        raise Exception("input Df should be of type Spark Dataframe, but given is of type {}".format(type(df)))

    if isinstance(tbl_name, str):
        try:
            tgt_tbl_df = spark.sql(f'select * from {tbl_name} limit 1')
            tgt_tbl_df_columns_list = tgt_tbl_df.columns
        except Exception as e:
            raise Exception(f"Spark sql target table {tbl_name} failed with exception {e}")
    elif isinstance(tbl_name, DataFrame):
        tgt_tbl_df = tbl_name
        tgt_tbl_df_columns_list = tbl_name.columns
    else:
        raise Exception(
            "Input table name should of type string/ Spark Dataframe, but given is of type {}".format(type(tbl_name)))

    input_df_columns_list = df.columns
 
    missing_columns = [x for x in tgt_tbl_df_columns_list if x.lower() not in [j.lower() for j in input_df_columns_list]]

    ## add all the missing columns in input df
    if len(missing_columns):
        for mis_col in missing_columns:
            df = df.withColumn(mis_col, lit(None))

    input_df_schema_json = json.loads(df.schema.json())['fields']

    ## add proper schema for the null columns in input df
    input_df_null_columns = []
    for inp_col in input_df_schema_json:
        if inp_col['type'] == 'null':
            input_df_null_columns.append(inp_col['name'])

    if input_df_null_columns:
        ncols_schema_json = json.loads(tgt_tbl_df.select(*input_df_null_columns).schema.json())['fields']
        for mtdt in ncols_schema_json:
            if mtdt['type'].lower() != 'null':
                df = df.withColumn(mtdt['name'], col(mtdt['name']).cast(mtdt['type']))

    final_df = df.select(*tgt_tbl_df_columns_list)

    return final_df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", "-t", type=str, required=True, help="<db>.<table-name> that needs to be backfilled.")
    parser.add_argument("--backfill", action="store_true", required=False)
    parser.add_argument("--int_cast", nargs="*", required=False)
    
    args = parser.parse_args()
    table = args.table
    is_backfill = args.backfill
    custom_cast_list = args.int_cast

    db, tname = table.split(".")
    if not (db or tname):
        raise Exception("Database or table name is missing in passed parameter.")

    tbl_details = _get_table(db, tname)

    bkfill_prefix = f'_bkfill_{datetime.strftime(datetime.now(),"%Y%m%d")}'
    location = tbl_details.get('Table').get('StorageDescriptor').get('Location')
    bkfill_table = f'{table}{bkfill_prefix}'
    bkfill_location = f'{location}{bkfill_prefix}'

    ## creating backfill table
    # generate table creation request
    if is_backfill:
        request = _create_table_request(tbl_details, bkfill_prefix)

        # check if backfill table already exists =>
        try:
            spark.sql(f"desc {bkfill_table}")
            spark.sql(f"drop table {bkfill_table}")
        except Exception as e:
            print(f"{bkfill_table} doesn't exist.")

        create_response = glue_client.create_table(DatabaseName=db, TableInput=request)
        if create_response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            print(f"{db}.{request.get('Name')} created successfully.")

        # backup table path
        bucket, key = _get_bucket_key(bkfill_location)
        s3_response = s3_client.put_object(Bucket=bucket, Key=key if key.endswith("/") else f'{key}/')
        if s3_response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            print(f"{bkfill_location} created successfully.")

        # casting and populating table
        src_df = spark.read.option("mergeSchema", True).parquet(location)
        col_order = spark.sql(f"select * from {bkfill_table} limit 1").columns

        # cast data types =>
        casted_df = _update_df_schema(spark, src_df, bkfill_table)
        if custom_cast_list:
            for cname_dtype in custom_cast_list:
                cname, dtype = cname_dtype.split("~")
                casted_df = casted_df.withColumn(cname, col(cname).cast(dtype))
            casted_df = _sync_data_type(spark, casted_df, bkfill_table)
        else:
            casted_df = _sync_data_type(spark, casted_df, bkfill_table)
        casted_df.select(*col_order).write.insertInto(bkfill_table, overwrite=True)

        print(f"{bkfill_table} population is done.")
    else:
        # overwriting the actual table
        bkfilled_df = spark.sql(f"select * from {bkfill_table}")
        target_col_order = spark.sql(f"select * from {table} limit 1").columns
        bkfilled_df.select(*target_col_order).write.insertInto(table, overwrite=True)
        print(f"{table} backfill is done.")

