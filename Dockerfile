FROM apache/airflow:2.1.2
COPY requirements.txt .
COPY --chown=airflow /jdk-11.0.12 /usr/java
ENV JAVA_HOME=/usr/java
RUN chmod +x /usr/java/*
ENV PATH=/home/airflow/.local/bin:/usr/local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/local/jdk-11.0.12/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
COPY --chown=airflow /spark /opt/spark
ENV SPARK_HOME=/opt/spark
RUN chmod +x /usr/spark/spark/*
# RUN chmod +x /opt/spark/*
RUN pip install -r requirements.txt