# Simple dockerfile para poder tener una instancia ligera de airflow

FROM apache/airflow:2.7.0

# Instalamos las dependencias necesarias
COPY requirements.txt /opt/airflow
RUN pip install -r /opt/airflow/requirements.txt

# Copiamos nuestro DAG
COPY --chown=airflow:root . /opt/airflow

RUN airflow db init
ENTRYPOINT airflow scheduler & airflow webserver
