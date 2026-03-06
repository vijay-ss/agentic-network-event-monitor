# FROM flink:1.18-scala_2.12-java11
FROM flink:1.19.0

RUN apt-get update -y && \
    apt-get install -y \
      default-jdk \
      python3 \
      python3-dev \
      python3-pip \
      wget \
      && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

RUN wget -q -P /opt/flink/lib \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar

RUN ln -s /usr/bin/python3 /usr/bin/python

RUN touch /config.yaml

WORKDIR /app
COPY flink/requirements.txt requirements.txt
COPY shared/  shared/
COPY flink/jobs/ /app/
RUN touch shared/__init__.py
RUN pip install -r requirements.txt