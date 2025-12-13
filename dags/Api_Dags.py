from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook


def api_comb(**context):
    import psycopg2
    import ast
    from requests import get
    import pendulum
    conn = BaseHook.get_connection(conn_id='conn_pg')

    connection = psycopg2.connect(
        dbname='etl',
        user=conn.login,
        password=conn.password,
        host=conn.host,
        port=conn.port
    )
    cur = connection.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS UserSkilF (
            lti_user_id TEXT,
            oauth_consumer_key TEXT,
            lis_result_sourcedid TEXT,
            lis_outcome_service_url TEXT,
            is_correct BOOLEAN,  
            attempt_type TEXT,
            created_at TIMESTAMP
        )
    ''')
    EndDate = pendulum.parse(context['ds']).subtract(days=1).to_date_string()
    StartDate = pendulum.parse(endDate).subtract(days=7).to_date_string()
    URL = 'https://b2b.itresume.ru/api/statistics'
    params = {'client': 'Skillfactory',
              'client_key': 'M2MGWS',
              'start': StartDate,
              'end': EndDate
              }
    result = get(url=URL, params=params)
    data = result.json()
    for el in data:
        insert_data = {
            'lti_user_id': el.get('lti_user_id'),
            'oauth_consumer_key': ast.literal_eval(el.get('passback_params')).get('oauth_consumer_key'),
            'lis_result_sourcedid': ast.literal_eval(el.get('passback_params')).get('lis_result_sourcedid'),
            'lis_outcome_service_url': el.get('passback_params').get('lis_outcome_service_url'),
            'is_correct': el.get('is_correct'),  # Может быть None/True/False
            'attempt_type': el.get('attempt_type'),
            'created_at': el.get('created_at')
        }

        # Выполняем INSERT
        cur.execute('''
                            INSERT INTO UserSkilF (
                                lti_user_id,
                                oauth_consumer_key,
                                lis_result_sourcedid,
                                lis_outcome_service_url,
                                is_correct,
                                attempt_type,
                                created_at
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ''', (
            insert_data['lti_user_id'],
            insert_data['oauth_consumer_key'],
            insert_data['lis_result_sourcedid'],
            insert_data['lis_outcome_service_url'],
            insert_data['is_correct'],
            insert_data['attempt_type'],
            insert_data['created_at']
        ))
        connection.commit()
def Tableagg(**context):
    import psycopg2
    conn = BaseHook.get_connection(conn_id='conn_pg')
    EndDate = pendulum.parse(context['ds']).subtract(days=1).to_date_string()
    StartDate = pendulum.parse(endDate).subtract(days=7).to_date_string()
    connection = psycopg2.connect(
        dbname='etl',
        user=conn.login,
        password=conn.password,
        host=conn.host,
        port=conn.port
    )
    cur = connection.cursor()
    cur.execute(
        """
        SELECT lti_user_id, created_at, count(is_correct) as total_At, sum(is_correct) as True_At
        FROM UserSkilF
        WHERE created_at >= %s
          AND created_at <= %s
          group by lti_user_id, created_at
        """,
        (StartDate, EndDate)  # Параметры передаются отдельно!
    )
    rows = cur.fetchall()
    column_names = [desc[0] for desc in cur.description]

    # 4. Формируем CSV в памяти
    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)
    writer.writerow(column_names)  # заголовки
    for row in rows:
        writer.writerow(row)  # данные

    # 5. Получаем параметры подключения к S3 через BaseHook
    s3_conn = BaseHook.get_connection('my_s3_conn')  # ваш Conn Id

    # Формируем параметры для boto3
    s3_params = {
        'aws_access_key_id': s3_conn.login,
        'aws_secret_access_key': s3_conn.password,
        'region_name': s3_conn.extra_dejson.get('region_name', 'ru-central1')  # регион из Extra
    }

    # 6. Загружаем в S3
    s3 = boto3.client('s3', **s3_params)

    s3.put_object(
        Bucket='ваш-бакет',
        Key='путь/к/файлу/output.csv',
        Body=csv_buffer.getvalue(),
        ContentType='text/csv'
    )

    # 7. Закрываем ресурсы
    csv_buffer.close()
    cur.close()
    connection.close()

    print("CSV успешно загружен в S3")

DEFAULT_ARGS = {
    'owner': 'admin',
    'retries': 2,
    'retry_delay': 600,
    'start_date': datetime(2025, 12, 12),
}
with DAG(
    dag_id="combineApiData77",
    tags=['admin', '4'],
    schedule='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    max_active_tasks=1
) as dag:

    dag_start = EmptyOperator(task_id='dag_start')
    dag_end = EmptyOperator(task_id='dag_end')
    ExtractApi =PythonOperator(task_id='dag_extract',python_callable=api_comb)
dag_start >> ExtractApi >> dag_end
