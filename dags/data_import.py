from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

DAG_ID = "school-lunch_data"

with DAG(
    DAG_ID,
    default_args={
        "depends_on_past": True,
        "retries": 1,
        "retry_delay": timedelta(seconds=3),
    },
    max_active_runs=1,
    max_active_tasks=5,
    description="wiki spark submit",
    schedule="0 3 1 * *", #매월 1일 새벽 3시에 실행
    start_date=datetime(2021, 1, 1),
    end_date=datetime(2021, 4, 30),
    catchup=True,
    tags=["spark", "submit"],
) as dag:
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule="all_done")
    
    
    ### 혹시 몰라서 코드는 킵해둠
    # def check_exists_meta():
    #     import os
    #     if os.path.exists(f'{RAW_BASE}/_SUCCESS'):
    #         return append_meta.task_id   
    #     else:
    #         return save_parquet.task_id
    
    # exists_meta = BranchPythonOperator(
    #     task_id="exists.meta",
    #     python_callable=check_exists_meta
    # )
    
    # SPARK_HOME= "/home/sgcho0907/app/spark-3.5.1-bin-hadoop3" # GCP
    # PY_PATH= "/home/sgcho/code/test/wiki_save_parquet.py" # LOCAL
    
    school_lunch = BashOperator(
        task_id='school.lunch',
        bash_command="""
            echo "DT=====> {{ execution_date.strftime('%Y%m') }}"
            
            spark-submit ~/code/school-lunch/code/school_lunch.py {{ execution_date.strftime('%Y%m') }}
            
            # 에러 처리: Spark 작업 실패 시 에러 코드 반환 및 로그 출력
            if [ $? -ne 0 ]; then
                echo "❌ Spark job failed!"

                # Discord 웹훅 알림 전송
                curl -H "Content-Type: application/json" \
                    -X POST \
                    -d '{
                        "username": "Airflow Alert",
                        "content": "🚨 *Spark 작업 실패*\nDAG: school-lunch\n날짜: {{ execution_date.strftime('%Y-%m') }}"
                        }' \
                    https://discordapp.com/api/webhooks/1362586291937612107/gXsqabc7FDZLsmEk23TwXINH89Q1m9zZb9pDevUEFopdePsjcyCEwiBYIIcwloSrKrnz

                exit 1
            fi
        """,        
        env={
            }
        )
    

    
    start >> school_lunch >> end