FROM apache/airflow:2.8.1

COPY requirements.txt .
RUN pip install apache-airflow[amazon,postgres]==${AIRFLOW_VERSION} -r requirements.txt

COPY dags /opt/airflow/dags
