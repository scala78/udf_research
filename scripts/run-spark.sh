#!/bin/bash
spark-submit --master yarn \
--deploy-mode cluster \
--name "UDFResearch" \
$(pwd)/spark_udf_research-assembly-0.1.0-SNAPSHOT.jar --class org.apache.spark.sql.spark_udf_research.UDFResearch