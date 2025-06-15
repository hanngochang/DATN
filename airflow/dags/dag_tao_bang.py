from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.state import DagRunState
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
import logging
import pandas as pd

import psycopg2
from psycopg2 import Error

import mysql.connector


def generate_create_table(schema, table, columns):
    pk_fields = []
    col_defs = []

    for col in columns:
        field, col_type, is_null, key, default, extra = col

        # Kiểu dữ liệu giữ nguyên từ MySQL DESCRIBE
        col_def = f"`{field}` {col_type}"

        # Null or Not Null
        if is_null == "NO":
            col_def += " NOT NULL"
        else:
            col_def += " NULL"

        # Default value
        if default is not None:
            if isinstance(default, str):
                col_def += f" DEFAULT '{default}'"
            else:
                col_def += f" DEFAULT {default}"

        # Extra (auto_increment)
        if extra:
            col_def += f" {extra.upper()}"

        col_defs.append(col_def)

        # Nếu là khóa chính thì lưu lại
        if key == "PRI":
            pk_fields.append(field)

    # Tạo phần khóa chính
    pk_clause = ""
    if pk_fields:
        pk_clause = f", PRIMARY KEY ({', '.join(f'`{f}`' for f in pk_fields)})"

    # Tổng hợp câu lệnh
    create_stmt = f"CREATE TABLE IF NOT EXISTS `{schema}`.`{table}` (\n  "
    create_stmt += ",\n  ".join(col_defs)
    create_stmt += pk_clause
    create_stmt += "\n);"

    return create_stmt

def connection(host, port, user, password): 
    conn = mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password
    )
    logging.info("Kết nối thành công")
    return conn, conn.cursor()

def get_info_table(**kwargs):
    conn, cursor = connection('103.141.144.236','3306','ivymoda','123456')
    cursor.execute("""
        SELECT TABLE_SCHEMA, TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE' and TABLE_SCHEMA in ('staging_ivy_moda_it');
    """)
    tables = cursor.fetchall()
    logging.info(f"Số lượng bảng: {len(tables)}")
    logging.info("Lấy dữ liệu thành công")

    # Danh sách lưu kết quả
    records = []

    for i, (schema, table) in enumerate(tables):
        logging.info(f"Xử lý bảng {i+1}/{len(tables)}: {schema}.{table}")
        try:
            cursor.execute(f"DESCRIBE `{schema}`.`{table}`")
            columns = cursor.fetchall()
            create_table_sql = generate_create_table(schema, table, columns)
            # Lưu vào list
            records.append({
                "schema": schema,
                "table": table,
                "create_sql": create_table_sql
            })
        except Exception as e:
            logging.warning(f"Lỗi khi DESCRIBE bảng {schema}.{table}: {e}")
        continue

    cursor.close()
    conn.close()
    df = pd.DataFrame(records)

    print(df.head())
    
    result_dict = df.to_dict(orient='records')

    kwargs['ti'].xcom_push(key='table_create_sqls', value=result_dict)

def create_schema(**kwargs):
    create_sql = kwargs['ti'].xcom_pull(task_ids='dag_create_sql_table', key='table_create_sqls')
    conn, cursor = connection('host.docker.internal','3308','hanglt','admin123')

    schemas = list(set([item['schema'] for item in create_sql]))
    logging.info(f"Các schema: {schemas}")

    for schema in schemas:
        try:
            sql = f"CREATE SCHEMA IF NOT EXISTS `{schema}`"
            logging.info(f"Thực thi: {sql}")
            cursor.execute(sql)
        except Exception as e:
            logging.warning(f"Lỗi khi tạo schema `{schema}`: {e}")
    logging.info(f"Đã tạo thành công các schema")
    cursor.close()
    conn.close()

def create_table(**kwargs):
    create_sql = kwargs['ti'].xcom_pull(task_ids='dag_create_sql_table', key='table_create_sqls')
    conn, cursor = connection('host.docker.internal','3308','hanglt','admin123')
    for i, item in enumerate(create_sql):
        try:
            sql = item['create_sql']
            logging.info(f"Thực thi lệnh {i+1}: {sql}")
            cursor.execute(f"DROP TABLE {item['schema']}.{item['table']}")
            cursor.execute(sql)
        except Exception as e:
            logging.warning(f"Lỗi khi thực thi SQL cho {item['schema']}.{item['table']}: {e}")
    cursor.close()
    conn.close()

def truncate_table(**kwargs):
    create_sql = kwargs['ti'].xcom_pull(task_ids='dag_create_sql_table', key='table_create_sqls')
    conn, cursor = connection('host.docker.internal','3308','hanglt','admin123')
    for i, item in enumerate(create_sql):
        try:
            sql = item['create_sql']
            logging.info(f"Thực thi lệnh {i+1}: {sql}")
            cursor.execute(f"TRUNCATE TABLE {item['schema']}.{item['table']}")
        except Exception as e:
            logging.warning(f"Lỗi khi thực thi SQL cho {item['schema']}.{item['table']}: {e}")
    cursor.close()
    conn.close()


default_args = {
    'owner': 'HangLT',                         # Owner của Dags (có thể sửa)
    'depends_on_past': False,                   # Ko sửa
    'start_date': datetime(2025, 5, 17),        # Ngày bắt đầu chạy luồng (nếu luồng chạy daily thì để ngày hiện tại - 1)
    # 'retries': 3,                               # Số lần retry để xử lý lỗi (có thể sửa)
    # 'retry_delay': timedelta(minutes=1),        # Thời gian delay giữa các lần retry (có thể sửa)
    # 'retry_exponential_backoff': True,          # Ko sửa
    # 'max_retry_delay': timedelta(minutes=5),    # Thời gian chờ task thực thi (có thể sửa)
}
with DAG(
    dag_id="dag_create_table",                   # Id của Dags (có thể sửa)
    default_args=default_args,                  
    schedule_interval=None,                     # Lập lịch cho job chạy (None: ko lập lịch, '@hourly': chạy hàng giờ, '@daily': chạy hàng ngày, '* * * * *': Tương ứng với  'phút giờ ngày_tháng tháng ngày_tuần')
    catchup=False,                              # Ko sửa
)as dag:
    dag_get_info_table = PythonOperator(
        task_id='dag_create_sql_table',
        python_callable=get_info_table,
        op_kwargs={},
    )
    # dag_create_schema = PythonOperator(
    #     task_id='dag_create_schema',
    #     python_callable=create_schema,
    #     op_kwargs={},
    # )
    # dag_create_table = PythonOperator(
    #     task_id='dag_create_table',
    #     python_callable=create_table,
    #     op_kwargs={},
    # )
    dag_truncate_table = PythonOperator(
        task_id='dag_truncate_table',
        python_callable=create_table,
        op_kwargs={},
    )
    #dag_get_info_table >> dag_create_schema >> dag_create_table
    dag_get_info_table >> dag_truncate_table