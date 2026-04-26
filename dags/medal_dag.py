import random
import time
from datetime import datetime

from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.utils.trigger_rule import TriggerRule

MYSQL_CONN_ID = "mysql_default"
TARGET_TABLE = "neo_data.mikk1max_hw_dag_results"
ATHLETE_TABLE = "olympic_dataset.athlete_event_results"

default_args = {
    "owner": "mikk1max",
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="medal_count_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["hw07"],
) as dag:

    # Task 1 — create result table
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

    # Task 2 — randomly pick one medal type and push it to XCom
    def _pick_medal(**context):
        medal = random.choice(["Bronze", "Silver", "Gold"])
        context["ti"].xcom_push(key="medal", value=medal)
        return medal

    pick_medal = PythonOperator(
        task_id="pick_medal",
        python_callable=_pick_medal,
    )

    # Task 3 — branch to the matching calc task
    def _pick_medal_task(**context):
        medal = context["ti"].xcom_pull(task_ids="pick_medal", key="medal")
        return f"calc_{medal}"

    pick_medal_task = BranchPythonOperator(
        task_id="pick_medal_task",
        python_callable=_pick_medal_task,
    )

    # Tasks 4a/4b/4c — count rows for each medal type and insert the result
    calc_bronze = MySqlOperator(
        task_id="calc_Bronze",
        mysql_conn_id=MYSQL_CONN_ID,
        sql=f"""
            INSERT INTO {TARGET_TABLE} (medal_type, count, created_at)
            SELECT 'Bronze', COUNT(*), NOW()
            FROM {ATHLETE_TABLE}
            WHERE medal = 'Bronze';
        """,
    )

    calc_silver = MySqlOperator(
        task_id="calc_Silver",
        mysql_conn_id=MYSQL_CONN_ID,
        sql=f"""
            INSERT INTO {TARGET_TABLE} (medal_type, count, created_at)
            SELECT 'Silver', COUNT(*), NOW()
            FROM {ATHLETE_TABLE}
            WHERE medal = 'Silver';
        """,
    )

    calc_gold = MySqlOperator(
        task_id="calc_Gold",
        mysql_conn_id=MYSQL_CONN_ID,
        sql=f"""
            INSERT INTO {TARGET_TABLE} (medal_type, count, created_at)
            SELECT 'Gold', COUNT(*), NOW()
            FROM {ATHLETE_TABLE}
            WHERE medal = 'Gold';
        """,
    )

    # Task 5 — deliberate delay (35 s → sensor will fail; use 5 s to pass)
    def _generate_delay(**_):
        time.sleep(35)

    generate_delay = PythonOperator(
        task_id="generate_delay",
        python_callable=_generate_delay,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    # Task 6 — sensor: the newest row must be no older than 30 seconds
    check_for_correctness = SqlSensor(
        task_id="check_for_correctness",
        conn_id=MYSQL_CONN_ID,
        sql=f"""
            SELECT TIMESTAMPDIFF(SECOND, MAX(created_at), NOW()) < 30
            FROM {TARGET_TABLE};
        """,
        mode="poke",
        poke_interval=5,
        timeout=60,
    )

    # Pipeline wiring
    create_table >> pick_medal >> pick_medal_task
    pick_medal_task >> [calc_bronze, calc_silver, calc_gold]
    [calc_bronze, calc_silver, calc_gold] >> generate_delay >> check_for_correctness
