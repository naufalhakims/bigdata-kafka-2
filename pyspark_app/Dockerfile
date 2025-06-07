# Use an official OpenJDK base image
FROM openjdk:11-jre-slim

# Set environment variables for Spark
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Install Python, pip, and dependencies
RUN apt-get update && \
    apt-get install -y python3 python3-pip curl procps && \
    rm -rf /var/lib/apt/lists/*

# Install PySpark and other Python libraries
RUN pip3 install pyspark==${SPARK_VERSION} pandas scikit-learn # scikit-learn for potential local model use, pandas for data handling

# Download and install Spark
RUN curl -o /tmp/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf /tmp/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} && \
    rm /tmp/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Set working directory
WORKDIR /app

# Copy the Spark script
COPY spark_model_trainer.py /app/spark_model_trainer.py

# Entrypoint will be overridden by docker-compose command