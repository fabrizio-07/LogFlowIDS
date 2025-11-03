FROM apache/spark:3.5.0

USER root

RUN apt-get update && apt-get install -y curl ca-certificates \
    && mkdir -p /opt/spark/jars \
    && rm -rf /var/lib/apt/lists/*

RUN curl -L -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
    curl -L -o /opt/spark/jars/kafka-clients-3.2.1.jar \
      https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.2.1/kafka-clients-3.2.1.jar && \
    curl -L -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar && \
    curl -L -o /opt/spark/jars/commons-pool2-2.11.1.jar \
      https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    curl -L -o /opt/spark/jars/lz4-java-1.8.0.jar \
      https://repo1.maven.org/maven2/org/lz4/lz4-java/1.8.0/lz4-java-1.8.0.jar && \
    curl -L -o /opt/spark/jars/snappy-java-1.1.10.5.jar \
      https://repo1.maven.org/maven2/org/xerial/snappy/snappy-java/1.1.10.5/snappy-java-1.1.10.5.jar && \
    curl -L -o /opt/spark/jars/zstd-jni-1.5.5-11.jar \
      https://repo1.maven.org/maven2/com/github/luben/zstd-jni/1.5.5-11/zstd-jni-1.5.5-11.jar

RUN groupadd -g 1001 sparkgroup || true && \
    useradd -m -u 1001 -g sparkgroup -d /home/sparkuser -s /bin/bash sparkuser || true

COPY src /opt/spark-app
RUN chown -R sparkuser:sparkgroup /opt/spark-app /opt/spark/jars /home/sparkuser

ENV HOME=/home/sparkuser
ENV SPARK_HOME=/opt/spark
ENV PYSPARK_PYTHON=python3

USER sparkuser

ENV PYSPARK_SUBMIT_ARGS="pyspark-shell"