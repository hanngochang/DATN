from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging
import mysql.connector

def connection(host, port, user, password):
    conn = mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password
    )
    return conn, conn.cursor()

def sync_views(**kwargs):
    # Kết nối nguồn
    src_conn, src_cursor = connection('103.141.144.236', '3306', 'ivymoda', '123456')
    # Kết nối đích
    dest_conn, dest_cursor = connection('host.docker.internal', '3308', 'hanglt', 'admin123')

    # Lấy danh sách view
    src_cursor.execute("""
        SELECT 
            table_schema,
            table_name,
            CONCAT('SHOW CREATE VIEW `', table_schema, '`.`', table_name, '`;') AS show_create_view_stmt
        FROM 
            information_schema.views
        WHERE 
            table_schema NOT IN ('sys', 'user_core');
    """)
    views = src_cursor.fetchall()
    logging.info(f"Đã tìm thấy {len(views)} view.")

    for i, (schema, view_name, show_stmt) in enumerate(views):
        logging.info(f"[{i+1}/{len(views)}] Xử lý view {schema}.{view_name}")
        try:
            src_cursor.execute(show_stmt)
            result = src_cursor.fetchone()
            if result and len(result) > 1:
                create_view_sql = result[1]
                create_view_sql = create_view_sql.replace("ivymoda`@`%", "hanglt`@`%")
                dest_cursor.execute(f"DROP VIEW IF EXISTS `{schema}`.`{view_name}`;")

                dest_cursor.execute(create_view_sql)
                logging.info(f"Đã tạo lại view {schema}.{view_name}")
            else:
                logging.warning(f"Không lấy được CREATE VIEW cho {schema}.{view_name}")

        except Exception as e:
            logging.error(f"Lỗi khi xử lý view {schema}.{view_name}: {e}")
            continue

    dest_conn.commit()
    src_cursor.close()
    dest_cursor.close()
    src_conn.close()
    dest_conn.close()
    logging.info("Tạo view hoàn tất.")

default_args = {
    'owner': 'HangLT',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 17),
}

with DAG(
    dag_id="sync_mysql_views_to_target",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    task_sync_views = PythonOperator(
        task_id='sync_mysql_views',
        python_callable=sync_views,
        provide_context=True
    )

    task_sync_views
