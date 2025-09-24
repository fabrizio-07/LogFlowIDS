FROM bitnami/spark:3.5.0

USER root

RUN apt-get update && apt-get install -y curl ca-certificates \
    && mkdir -p /opt/spark/jars \
    && rm -rf /var/lib/apt/lists/*

RUN curl -L -o /opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
    curl -L -o /opt/bitnami/spark/jars/kafka-clients-3.2.1.jar \
      https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.2.1/kafka-clients-3.2.1.jar && \
    curl -L -o /opt/bitnami/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar && \
    curl -L -o /opt/bitnami/spark/jars/commons-pool2-2.11.1.jar \
      https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    curl -L -o /opt/bitnami/spark/jars/lz4-java-1.8.0.jar \
      https://repo1.maven.org/maven2/org/lz4/lz4-java/1.8.0/lz4-java-1.8.0.jar && \
    curl -L -o /opt/bitnami/spark/jars/snappy-java-1.1.10.5.jar \
      https://repo1.maven.org/maven2/org/xerial/snappy/snappy-java/1.1.10.5/snappy-java-1.1.10.5.jar && \
    curl -L -o /opt/bitnami/spark/jars/zstd-jni-1.5.5-11.jar \
      https://repo1.maven.org/maven2/com/github/luben/zstd-jni/1.5.5-11/zstd-jni-1.5.5-11.jar

COPY src /opt/spark-app
RUN chown -R 1001:0 /opt/spark-app /opt/spark/jars

USER 1001

ENV PYSPARK_SUBMIT_ARGS="pyspark-shell"