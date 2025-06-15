from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, date
import logging
import pandas as pd
import mysql.connector

def connection(host, port, user, password):
    try:
        conn = mysql.connector.connect(
            host=host,
            port=port,
            user=user,
            password=password
        )
        logging.info("Kết nối thành công tới MySQL")
        return conn, conn.cursor()
    except mysql.connector.Error as e:
        logging.error(f"Lỗi kết nối MySQL: {e}")
        raise

def call_procedure(**kwargs):
    # Kết nối tới MySQL
    conn, cursor = connection('host.docker.internal', '3308', 'hanglt', 'admin123')
    
    # Đọc file CSV chứa danh sách stored procedures
    procedures = pd.read_csv('/opt/airflow/dags/procedure_airflow.csv')
    
    # Lấy ngày hiện tại và ngày hôm sau
    from_date = date.today() - timedelta(days=14)
    to_date = date.today() 
    
    try:
        for index, row in procedures.iterrows():
            schema = row['schema']
            proc = row['procedure']
            param1 = row['param1']
            param2 = row['param2']
            
            # Kiểm tra xem param1 và param2 có giá trị không
            if pd.notna(param1) and pd.notna(param2):
                logging.info(f"Gọi stored procedure: {schema}.{proc} với from_date={from_date}, to_date={to_date}")
                cursor.callproc(f"{schema}.{proc}", [from_date, to_date])
            else:
                logging.info(f"Gọi stored procedure: {schema}.{proc} không có tham số")
                cursor.callproc(f"{schema}.{proc}", [])
        
        # Commit các thay đổi
        conn.commit()
        logging.info("Tất cả stored procedures đã được gọi thành công")
        
    except mysql.connector.Error as e:
        logging.error(f"Lỗi khi gọi stored procedure: {e}")
        raise
    finally:
        cursor.close()
        conn.close()
        logging.info("Đã đóng kết nối MySQL")

default_args = {
    'owner': 'HangLT',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 17),
    #'retries': 3,
    #'retry_delay': timedelta(minutes=1),
    'retry_exponential_backoff': True,
    #'max_retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="dag_call_procedure",
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:
    dag_call_procedure = PythonOperator(
        task_id='dag_call_procedure',
        python_callable=call_procedure,
        op_kwargs={},
    )

    dag_call_procedure