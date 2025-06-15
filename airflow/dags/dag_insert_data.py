from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.state import DagRunState
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
import logging
import pandas as pd
import time

import psycopg2
from psycopg2 import Error

import mysql.connector


def connection(host, port, user, password): 
    conn = mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password
    )
    logging.info("Kết nối thành công")
    return conn, conn.cursor()

def get_data_src(**kwargs):
    conn, cursor = connection('103.141.144.236','3306','ivymoda','123456')
    cursor.execute("""
        SELECT TABLE_SCHEMA, TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE' and TABLE_SCHEMA not in ('mysql', 'performance_schema', 'sys');
    """)
    tables = cursor.fetchall()
    logging.info(f"Số lượng bảng: {len(tables)}")
    logging.info("Lấy dữ liệu thành công")

    cursor.close()
    conn.close()
    df = pd.DataFrame(tables)
    df = df.iloc[128:]
    print(df.head())
    
    result_dict = df.to_dict(orient='records')

    kwargs['ti'].xcom_push(key='table_name', value=result_dict)

def insert_data(**kwargs):
    table_name = kwargs['ti'].xcom_pull(task_ids='dag_get_data_src', key='table_name')
    conn, cursor = connection('103.141.144.236', '3306', 'ivymoda', '123456')
    conn1, cursor1 = connection('host.docker.internal', '3308', 'hanglt', 'admin123')

    logging.info("Kết nối thành công Ivymoda")
    logging.info("Kết nối thành công HangLT")
    batch_size =1000
    for i, item in enumerate(table_name):
        try:
            schema = item['0']
            table = item['1']

            logging.info(f"Thực thi bảng {i} insert từ: {schema}.{table}")
            cursor.execute(f"SELECT * FROM `{schema}`.`{table}`")
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]

            # Tạo câu lệnh INSERT
            placeholders = ', '.join(['%s'] * len(columns))
            columns_str = ', '.join([f"`{col}`" for col in columns])
            insert_query = f"INSERT INTO `{schema}`.`{table}` ({columns_str}) VALUES ({placeholders})"

            logging.info(f"Tạo câu lệnh insert {len(rows)} rows vào {schema}.{table}")
            cursor1.execute(f"TRUNCATE TABLE `{schema}`.`{table}`")
            logging.info(f"TRUNCATE bảng {schema}.{table} thành công")
            for batch_start in range(0, len(rows), batch_size):
                batch = rows[batch_start:batch_start+batch_size]
                cursor1.executemany(insert_query, batch)        #Chưa test
            logging.info(f"Insert dữ liệu vào bảng {schema}.{table} thành công")
            # for row in rows:
            #     cursor1.execute(insert_query, row)
            conn1.commit()
            time.sleep(10)

        except Exception as e:
            logging.warning(f"Lỗi khi xử lý bảng {schema}.{table}: {e}")

    cursor.close()
    conn.close()
    cursor1.close()
    conn1.close()

def get_procedure_src(**kwargs):
    conn, cursor = connection('103.141.144.236','3306','ivymoda','123456')
    cursor.execute("""
         SELECT 
            ROUTINE_SCHEMA,
            ROUTINE_NAME
        FROM 
            INFORMATION_SCHEMA.ROUTINES
        WHERE 
            ROUTINE_TYPE = 'PROCEDURE' 
            AND ROUTINE_SCHEMA NOT IN ('mysql', 'performance_schema', 'sys');
    """)
    procedures_list = cursor.fetchall()
    logging.info(f"Số lượng procedure: {len(procedures_list)}")
    logging.info("Lấy tên procedure thành công")

    records = []

    for i, item in enumerate(procedures_list):
        try:
            schema = item[0]
            procedure = item[1]

            logging.info(f"Thực thi lấy câu lệnh tạo procedure thứ {i} của: {schema}.{procedure}")
            cursor.execute(f"SHOW CREATE PROCEDURE `{schema}`.`{procedure}`;")
            create_data = cursor.fetchone()

            create_sql = create_data[2]

            lines = create_sql.split('\n')
            lines = [line for line in lines if not line.strip().startswith('DEFINER=')]
            create_sql = '\n'.join(lines)

            if f"`{schema}`.`{procedure}`" not in create_sql:
                create_sql = create_sql.replace(
                    f"PROCEDURE `{procedure}`",
                    f"PROCEDURE `{schema}`.`{procedure}`"
                )

            # Loại bỏ DELIMITER (sai cú pháp trong API) và đảm bảo kết thúc bằng dấu ;
            create_sql = create_sql.strip().rstrip(';') + ';'
            drop_sql = f"DROP PROCEDURE IF EXISTS `{schema}`.`{procedure}`;"

            records.append({
                "schema": schema,
                "procedure": procedure,
                "drop_sql": drop_sql,
                "create_sql": create_sql
            })


        except Exception as e:
            logging.warning(f"Lỗi khi xử lý procedure {item[0]}.{item[1] if len(item) > 1 else 'UNKNOWN'}: {e}")

    cursor.close()
    conn.close()

    df = pd.DataFrame(records)
    print(df.head())

    result_dict = df.to_dict(orient='records')
    kwargs['ti'].xcom_push(key='procedure_name', value=result_dict)


def create_procedure(**kwargs):
    conn, cursor = connection('host.docker.internal', '3308', 'hanglt', 'admin123')
    procedure_name = kwargs['ti'].xcom_pull(task_ids='dag_get_procedure_src', key='procedure_name')

    for i, item in enumerate(procedure_name):
        try:
            sql = item['create_sql']
            sql = sql.replace("`ivymoda`@`%`", "`hanglt`@`%`")
            schema =item['schema']
            procedure = item['procedure']
            drop_sql = item['drop_sql']
            logging.info(f"Thực thi lệnh tạo procedure {i+1}: {schema}.{procedure}")
            cursor.execute(drop_sql)
            cursor.execute(sql)
        except Exception as e:
            logging.warning(f"Lỗi khi thực thi SQL cho {item['schema']}.{item['procedure']}: {e}")
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
    dag_id="dag_insert_data_from_src",                   # Id của Dags (có thể sửa)
    default_args=default_args,                  
    schedule_interval=None,                     # Lập lịch cho job chạy (None: ko lập lịch, '@hourly': chạy hàng giờ, '@daily': chạy hàng ngày, '* * * * *': Tương ứng với  'phút giờ ngày_tháng tháng ngày_tuần')
    catchup=False,                              # Ko sửa
)as dag:
    # dag_get_data_src = PythonOperator(
    #     task_id='dag_get_data_src',
    #     python_callable=get_data_src,
    #     op_kwargs={},
    # )
    # dag_create_schema = PythonOperator(
    #     task_id='dag_insert_data',
    #     python_callable=insert_data,
    #     op_kwargs={},
    # )
    dag_get_procedure_src = PythonOperator(
        task_id='dag_get_procedure_src',
        python_callable=get_procedure_src,
        op_kwargs={},
    )
    dag_create_procedure = PythonOperator(
        task_id='dag_create_procedure',
        python_callable=create_procedure,
        op_kwargs={},
    )
    #dag_get_data_src >> dag_create_schema >> 
    dag_get_procedure_src >> dag_create_procedure
