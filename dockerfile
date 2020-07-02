FROM puckel/docker-airflow:latest
COPY ./requirements.txt /usr/local/.requirements.txt
RUN pip install --user psycopg2-binary
RUN pip install -r /usr/local/.requirements.txt
ENV AIRFLOW_HOME=/usr/local/airflow
COPY ./airflow.cfg /usr/local/airflow/airflow.cfg
ENV PATH="/usr/local/airflow:${PATH}"
ENV AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY
ENV AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
