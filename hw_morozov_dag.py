from datetime import datetime
import pandas as pd
import psycopg2

from airflow.operators.dummy_operator import DummyOperator
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.sql_sensor import SqlSensor


# Набор аргументов по умолчанию для DAG
args = {
    'owner':'morozov_ea',
    'start_date': datetime(2022,1,12),
    'provide_context':True
}

        # Подключение к базе данных "по рабоче-крестьянски",
        # чтобы не пользоваться хуками и Connections на стартовом этапе обучения
# pg_connection = {
#     'host':'host.docker.internal',
#     'port':'65432',
#     'user':'postgres',
#     'password':'password',
#     'database':'test'
# }

# что-нибудь можно положить в Variable для тренировки
hw_url = Variable.get('hw_url')

# --------------------   используемые функции   --------------------

def hello():
    print('Airflow homework performed by Morozov E.A.')

def get_conn_credentials(connection_id):
    connection = BaseHook.get_connection(connection_id)
    return connection

def connect_to_psql(**kwargs):
    # кусок кода пока подключение к БД было по глобальным переменным из pg_connection вначале
    # connection = psycopg2.connect(
    #     host=pg_connection.get('host'),
    #     port=pg_connection.get('port'),
    #     user=pg_connection.get('user'),
    #     password=pg_connection.get('password'),
    #     database=pg_connection.get('database')
    #     )

    ti = kwargs['ti']
    # подключение переоформлено с глобальной переменной на Hook и Variable, доступные в Airflow
    connection_name = Variable.get('connection_id')
    pg_db_conn = get_conn_credentials(connection_name)

    pg_db_hostname, pg_db_port, pg_db_username, pg_db_pass, pg_db = (
        pg_db_conn.host,
        pg_db_conn.port,
        pg_db_conn.login,
        pg_db_conn.password,
        pg_db_conn.schema
        )
    
    ti.xcom_push(
        value=[pg_db_hostname, pg_db_port, pg_db_username, pg_db_pass, pg_db],
        key='pg_db_conn'
        )

    connection = psycopg2.connect(
        host=pg_db_hostname,
        port=pg_db_port,
        user=pg_db_username,
        password=pg_db_pass,
        database=pg_db
        )
    
    cursor = connection.cursor()

    cursor.execute('CREATE TABLE IF NOT EXISTS test_table (id serial PRIMARY KEY, num integer, data varchar);')
    cursor.execute('INSERT INTO test_table (num, data) VALUES (%s, %s)',(100, 'morozov'))

    connection.commit()
    cursor.close()
    connection.close()
    

def read_from_psql(**kwargs):
    # подключение переоформлено с глобальной переменной на Hook и Variable, доступные в Airflow
    ti = kwargs['ti']
    
    pg_db_hostname, pg_db_port, pg_db_username, pg_db_pass, pg_db = ti.xcom_pull(
        key='pg_db_conn',
        task_ids='connection_to_psql_db'
        )
    
    connection = psycopg2.connect(
        host=pg_db_hostname,
        port=pg_db_port,
        user=pg_db_username,
        password=pg_db_pass,
        database=pg_db
        )
    
    cursor = connection.cursor()

    cursor.execute('SELECT * FROM test_table;')

    print(cursor.fetchone())
    cursor.close()
    connection.close()

# ______________
# функция для эмуляции таблицы с временными данными, а также функции для выполнения в случае успеха\неудачи
def tmp_psql_table(**kwargs):
    ti = kwargs['ti']
    
    pg_db_hostname, pg_db_port, pg_db_username, pg_db_pass, pg_db = ti.xcom_pull(
        key='pg_db_conn'
        )
    
    connection = psycopg2.connect(
        host=pg_db_hostname,
        port=pg_db_port,
        user=pg_db_username,
        password=pg_db_pass,
        database=pg_db
        )
    
    cursor = connection.cursor()

    cursor.execute('CREATE TABLE IF NOT EXISTS test_table_tmp (id serial PRIMARY KEY, num integer, data varchar);')
    cursor.execute('INSERT INTO test_table_tmp (num, data) VALUES (%s, %s)', (200, 'morodze'))
    cursor.execute('INSERT INTO test_table_tmp (num, data) VALUES (%s, %s)', (300, 'ivanenko'))
    cursor.execute('INSERT INTO test_table_tmp (num, data) VALUES (%s, %s)', (400, 'petrov'))

    connection.commit()
    cursor.close()
    connection.close()

def _success_variant():
    return print('Можно объединить записи')

def _failure_variant():
    return print('Новых записей нет, добавлять нечего')

# ______________

def csv_load(**kwargs):
    ti = kwargs['ti']
    df = pd.read_csv(
        hw_url, 
        index_col=0, 
        header='infer'
        ).reset_index(drop=True)
    n = len(df)
    print('\n\t\tЗадача 12.a')
    print(f'''
          Скачайте произвольный csv-файл и напишите в python-операторе код,
          который подключается к скачанному csv-файлу и подсчитывает количество
          строк в файле. Попробуйте использовать технологию Variables и записать
          путь к файлу в список переменных Airflow, а кол-во строк в файле
          записать в XCom.''')
    print('\n\tКоличество строк в файле =', n)
    
        # ниже я несколько раз пытался запушить в xcom датафрейм df целиком,
        # что приводило лишь к ошибкам вида:
        # "ERROR - Could not serialize the XCom value into JSON. If you are using
        # pickle instead of JSON for XCom, then you need to enable pickle support
        # for XCom in your *** config. 
        # [2022-12-02, 15:14:03 UTC] {taskinstance.py:1851} ERROR - Task failed 
        # with exception", - что по всей видимости говорит о невозможности
        # передавать в xcom объекты не json-формата которые вдобавок могут
        # оказаться сколь-нибудь большими. Передадим только величину "n"
    
    ti.xcom_push(key='df_length', value=n)

def df_constr(**kwargs):
    ti = kwargs['ti']
    df = pd.read_csv(
        hw_url, 
        index_col=0, 
        header='infer'
        ).reset_index(drop=True)
    df['new_col'] = df.index
    n = ti.xcom_pull(key='df_length')
    df['new_col'] = n-df['new_col'] 
    print('\n\t\tЗадача 12.b')
    print(f'''
          b. Создайте еще один python-оператор, передайте в него из первого
          python-оператора количество строк. Прочитайте файл(с использованием
          переменных) и добавьте к данному файлу колонку справа, которая будет
          номеровать строки в обратном порядке(т.е. первая строка должна принять
          значение кол-ва строк файла, каждая следующая строка -1 от значения в
          предыдущей). Сохраните полученный файл.''')
    
    print(f'\nПолученная таблица (фрагмент):\n {df.head(5)}')
    df.to_csv(path_or_buf='./dags/out.csv', sep=',', na_rep='empty')
    # df.to_csv(path_or_buf='./out.csv', sep=',', na_rep='empty')
    print(f'''Файл out.csv располагается в папке dags c именем out.csv''')

        # Сделал весьма костыльным способом, на мой взгляд, долго разбирался
        # с файловой структурой и в итоге сохранил результат в папку с dags.
        # Понимания, как лучше забрать файл у workera и переложить его в папку
        # у меня не сформировалось. Предпринимались различные попытки: скопировать
        # по внутренней сети Docker между контейнерами по их IP-адресам, с пом.
        # bash-комманд, через AirflowConnections - нигде не получилось нормально...

# --------------------   DAG   --------------------
with DAG(
    dag_id = 'hw_morozov_dag',
    schedule = '0/30 0/2 * * *',
    catchup = False,
    default_args = args
) as dag:

    #Tasks are represented as operators
    hello_task = PythonOperator(
        task_id = 'hello_python',
        python_callable = hello
        )

    csv_load_task = PythonOperator(
        task_id = 'csv_load',
        python_callable = csv_load
        )

    df_constr_task = PythonOperator(
        task_id = 'df_construction',
        python_callable = df_constr
        )

        # "12.d. Добавьте bash-оператор, который будет печатать «Success», если все предыдущие шаги
        # прошли удачно." - выполнить эту часть c FileSensor в итоге получилось кое-как.

    file_sensing = FileSensor(
        task_id = 'file_sensing_task',
        filepath = 'out.csv',
        fs_conn_id = 'file_path',
        poke_interval = 20
        )

    bash_task_1 = BashOperator(
        task_id = 'bash_mkdir_echo_1',
        bash_command = 'echo Задание 12.с Создайте Bash-оператор, который переместит файл в папку.'
        )

    bash_task_2 = BashOperator(
        task_id = 'bash_mkdir_echo_2',
        bash_command = 'echo Задание выполнено в рамках вышестоящего python-оператора, поскольку возникли сложности в работе с bash-операторами, создающими папки внутри контейнеров Docker.'
        )

        # bash-операторы ниже не желали создавать папки за пределами контейнера и копировать файлы
        # хотя в терминале, запущенном в контейнере docker всё работало, закоментил

    # bash_mkdir_task_1 = BashOperator(
    #     task_id = 'bash_mkdir',
    #     bash_command = 'mkdir ./result')

    # bash_mkdir_task_2 = BashOperator(
    #     task_id = 'bash_cp_file',
    #     bash_command = 'cp ./dags/out.csv ./dags/result/')

    # task-и для работы с БД

    conn_to_sql_task = PythonOperator(
        task_id = 'connection_to_psql_db',
        python_callable = connect_to_psql
        )

    read_from_sql_task = PythonOperator(
        task_id = 'read_data_from_psql_db',
        python_callable = read_from_psql
        )

        # задание поменялось в ходе выполнения, но работу затирать не хотелось, поэтому ниже 
        # оставляю SqlSensor с попыткой осмысленного применения оного. Логику использования
        # придумал только следующую: если есть сторонняя заполненная таблица, то можно добавить
        # записи из неё в основную и очистить эту временную таблицу, однако внедрить это решение
        # не вышло из-за возникавших ошибок (отладить не успевал)
    
    conn_to_tmp_sql_task = PythonOperator(
        task_id = 'connection_to_tmp_psql_db',
        python_callable = tmp_psql_table
        )
    
    sql_sensor = SqlSensor(
        task_id ='sql_tmp_check',
        poke_interval = 60,
        timeout = 180,
        soft_fail = True,
        retries = 2,
        sql = 'SELECT COUNT(*) FROM test_table',
        success = _success_variant,
        failure = _failure_variant,
        conn_id = Variable.get('connection_id'),
        dag=dag
    )

    bash_task_3 = BashOperator(
        task_id = 'bash_succses',
        bash_command = 'echo Данные в общей БД обновлены, временная таблица очищена'
        )


# --------------------   прописываем порядок исполнения task'ов   --------------------       
    hello_task >> csv_load_task >> df_constr_task >> file_sensing >> bash_task_1 >> bash_task_2
    hello_task >> conn_to_sql_task >> read_from_sql_task >> bash_task_3
    conn_to_tmp_sql_task >> sql_sensor >> bash_task_3



