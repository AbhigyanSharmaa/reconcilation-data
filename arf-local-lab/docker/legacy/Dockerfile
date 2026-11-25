# The "Databricks" container for the local lab:
# Python 3.11 + Java 17 + PySpark 3.5 + Delta Lake 3.3 + JupyterLab + the BigQuery client.
FROM python:3.11-slim-bookworm

# Java runs Spark; procps provides `ps`, which Spark's launch scripts use.
RUN mkdir -p /usr/share/man/man1 \
 && apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && rm -rf /var/lib/apt/lists/*

ENV TZ=UTC \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/arf

COPY requirements-lab.txt /tmp/requirements-lab.txt
RUN pip install --no-cache-dir -r /tmp/requirements-lab.txt

# Download the Delta Lake jars once, at build time, into /opt/ivy.
# arf-config/settings.yaml points spark.jars.ivy at the same folder, so later sessions reuse them.
RUN python -c "from delta import configure_spark_with_delta_pip; from pyspark.sql import SparkSession; \
s = configure_spark_with_delta_pip(SparkSession.builder.master('local[1]').config('spark.jars.ivy', '/opt/ivy')).getOrCreate(); \
print('Spark', s.version, 'with Delta ready'); s.stop()"

WORKDIR /lab
EXPOSE 8888 4040
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", \
     "--ServerApp.token=arf", "--ServerApp.root_dir=/lab"]
