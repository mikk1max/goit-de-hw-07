import random
import time
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.sensors.sql import SqlSensor
from airflow.utils.trigger_rule import TriggerRule

MYSQL_CONN_ID = "goit_mysql_db_shepeta"
TARGET_TABLE = "neo_data.shepeta_hw_dag_results"
ATHLETE_TABLE = "olympic_dataset.athlete_event_results"

default_args = {
    "owner": "shepeta",
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="shepeta_medal_count_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["hw07", "shepeta"],
) as dag:

    create_table = MySqlOperator(
        task_id="create_table",
        mysql_conn_id=MYSQL_CONN_ID,
        sql=f"""
            CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
                id          INT AUTO_INCREMENT PRIMARY KEY,
                medal_type  VARCHAR(10) NOT NULL,
                count       INT NOT NULL,
                created_at  DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """,
    )

    def _pick_medal(**context):
        medal = random.choice(["Bronze", "Silver", "Gold"])
        print(f"Selected medal: {medal}")
        context["ti"].xcom_push(key="medal", value=medal)
        return medal

    pick_medal = PythonOperator(
        task_id="pick_medal",
        python_callable=_pick_medal,
    )

    def _pick_medal_task(**context):
        medal = context["ti"].xcom_pull(task_ids="pick_medal", key="medal")
        return f"calc_{medal}"

    pick_medal_task = BranchPythonOperator(
        task_id="pick_medal_task",
        python_callable=_pick_medal_task,
    )

    calc_Bronze = MySqlOperator(
        task_id="calc_Bronze",
        mysql_conn_id=MYSQL_CONN_ID,
        sql=f"""
            INSERT INTO {TARGET_TABLE} (medal_type, count, created_at)
            SELECT 'Bronze', COUNT(*), NOW()
            FROM {ATHLETE_TABLE}
            WHERE medal = 'Bronze';
        """,
    )

    calc_Silver = MySqlOperator(
        task_id="calc_Silver",
        mysql_conn_id=MYSQL_CONN_ID,
        sql=f"""
            INSERT INTO {TARGET_TABLE} (medal_type, count, created_at)
            SELECT 'Silver', COUNT(*), NOW()
            FROM {ATHLETE_TABLE}
            WHERE medal = 'Silver';
        """,
    )

    calc_Gold = MySqlOperator(
        task_id="calc_Gold",
        mysql_conn_id=MYSQL_CONN_ID,
        sql=f"""
            INSERT INTO {TARGET_TABLE} (medal_type, count, created_at)
            SELECT 'Gold', COUNT(*), NOW()
            FROM {ATHLETE_TABLE}
            WHERE medal = 'Gold';
        """,
    )

    # DELAY_SECONDS = 35  →  sensor FAILS  (demo for homework)
    # DELAY_SECONDS = 5   →  sensor PASSES (demo for homework)
    DELAY_SECONDS = 5

    def _generate_delay(**_):
        print(f"Sleeping {DELAY_SECONDS}s ...")
        time.sleep(DELAY_SECONDS)
        print("Done sleeping")

    generate_delay = PythonOperator(
        task_id="generate_delay",
        python_callable=_generate_delay,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    check_for_correctness = SqlSensor(
        task_id="check_for_correctness",
        conn_id=MYSQL_CONN_ID,
        sql=f"""
            SELECT CASE
                WHEN MAX(created_at) IS NOT NULL
                     AND TIMESTAMPDIFF(SECOND, MAX(created_at), NOW()) <= 30
                THEN 1
                ELSE 0
            END
            FROM {TARGET_TABLE};
        """,
        mode="poke",
        poke_interval=5,
        timeout=60,
    )

    create_table >> pick_medal >> pick_medal_task
    pick_medal_task >> [calc_Bronze, calc_Silver, calc_Gold]
    [calc_Bronze, calc_Silver, calc_Gold] >> generate_delay >> check_for_correctness
