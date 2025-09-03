FROM apache/airflow:3.0.1

USER root

# RUN apt-get update && apt-get install -y procps openjdk-21-jdk
RUN apt-get update && apt-get install -y openjdk-17-jdk

RUN apt-get install -y git


COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

USER airflow
