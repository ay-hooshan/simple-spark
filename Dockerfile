FROM ubuntu:latest

WORKDIR /app

# install openjdk-8 wget and scala
RUN \
    apt-get update && \
    apt-get install -y openjdk-8-jdk wget scala && \
    rm -rf /var/lib/apt/lists/*

# install python
RUN \
    apt-get update && \
    apt-get install -y python python-dev python-pip python-virtualenv && \
    rm -rf /var/lib/apt/lists/*

# download spark folder
RUN wget https://mirrors.estointernet.in/apache/spark/spark-3.1.2/spark-3.1.2-bin-hadoop3.2.tgz && \
    tar -zxf /spark-* -C /opt/spark && \
    rm spark-*.tgz

COPY main.py .
COPY requirements.txt . 

# install requirements
RUN \
    pip install --upgrade pip && \
    pip install -r requirements.txt

RUN ./spark-3.1.2-bin-hadoop3.2/sbin/start-master.sh 
RUN ./spark-3.1.2-bin-hadoop3.2/sbin/start-worker.sh spark://${HOST_NAME}:7077

# running spark app
CMD ["./spark-3.1.2-bin-hadoop3.2/bin/spark-submit --master spark://${HOST_NAME}:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 ./main.py"]