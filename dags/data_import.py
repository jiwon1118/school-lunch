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
    start_date=datetime(2021, 2, 1),
    end_date=datetime(2025, 5, 30),
    catchup=True,
    tags=["spark", "submit"],
) as dag:
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule="all_done")
    
    school_lunch = BashOperator(
        task_id='school.lunch',
        bash_command="""
            echo "DT=====> {{ execution_date.strftime('%Y%m') }}"
            
            #airflow 서버로 가상환경 전달
            export PYSPARK_PYTHON=/home/ubuntu/.pyenv/versions/air/bin/python
            
            #requirements.txt를 이용하여 airflow 가상환경에 필요 패키지 설치
            /home/ubuntu/.pyenv/versions/air/bin/pip install -r /home/ubuntu/code/school-lunch/requirements.txt
            
            #spark 작업 요청
            /home/ubuntu/app/spark-3.5.5-bin-hadoop3/bin/spark-submit \
            --master spark://127.0.0.1:7077 \
            --driver-memory 2g \
            --executor-memory 2g \
            --executor-cores 2 \
            ~/code/school-lunch/code/school_lunch.py {{ execution_date.strftime('%Y%m') }}
            
            # 에러 처리: Spark 작업 실패 시 에러 코드 반환 및 로그 출력
            if [ $? -ne 0 ]; then
                echo "❌ Spark job failed!"

                # Discord 웹훅 알림 전송
                curl -H "Content-Type: application/json" \
                -X POST \
                -d "{"username": "Airflow Alert", "content": "🚨 *Spark 작업 실패* DAG: school-lunch"}" \
                https://discordapp.com/api/webhooks/1362586291937612107/gXsqabc7FDZLsmEk23TwXINH89Q1m9zZb9pDevUEFopdePsjcyCEwiBYIIcwloSrKrnz

                exit 1
            fi
        """,        
        env={
            }
        )
    
    start >> school_lunch >> end
    
    