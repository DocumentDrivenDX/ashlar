#!/bin/sh
# Complete public CLI template. Replace every REPLACE_WITH value before use.
# Invoke through your explicit finite process supervisor; this CLI runs Spark directly.
set -eu
exec /usr/bin/env -i \
  PATH=/usr/bin:/bin \
  JAVA_HOME='/absolute/REPLACE_WITH_JDK21' \
  PYSPARK_PYTHON='/absolute/REPLACE_WITH_QUERY_ENV/bin/python' \
  SPARK_LOCAL_IP=127.0.0.1 \
  PYTHONNOUSERSITE=1 PYTHONDONTWRITEBYTECODE=1 \
  '/absolute/REPLACE_WITH_QUERY_ENV/bin/python' -I -B -m ashlar \
  query-commerce-count-star \
  --postgres-profile explicit-loopback \
  --case expanded-count-having \
  --index /absolute/REPLACE_WITH_WEFT041_INDEX.json \
  --installation /absolute/REPLACE_WITH_WEFT041_INSTALLATION \
  --publication /absolute/REPLACE_WITH_ORIGINAL_PUBLICATION \
  --output /absolute/REPLACE_WITH_FRESH_QUERY_OUTPUT \
  --jars /absolute/REPLACE_WITH_EXACT_DELTA4_TWO_JAR_DIRECTORY \
  --model /absolute/REPLACE_WITH_ORIGINAL_MODEL.json \
  --graph /absolute/REPLACE_WITH_ORIGINAL_GRAPH.json \
  --umf-source /absolute/REPLACE_WITH_CLEAN_UMF_SOURCE \
  --bun /absolute/REPLACE_WITH_BUN \
  --git /absolute/REPLACE_WITH_GIT \
  --pack /absolute/REPLACE_WITH_ORIGINAL_PACK.json \
  --postgres-container REPLACE_WITH_EXPLICIT_PRIVATE_FIXTURE_CONTAINER_NAME \
  --postgres-host 127.0.0.1 \
  --postgres-port REPLACE_WITH_OBSERVED_LOOPBACK_PORT \
  --postgres-database truss_e2e \
  --producer-timeout-seconds 30 \
  --producer-maximum-output-bytes 1048576 \
  --producer-maximum-receipt-bytes 4194304 \
  --maximum-artifact-bytes 16777216 \
  --maximum-rows 1000 \
  --maximum-cell-bytes 16777216 \
  --maximum-total-cell-bytes 67108864 \
  --session-provider /absolute/REPLACE_WITH_COMPLETED_QUERY_PROVIDER.py \
  --session-provider-sha256 REPLACE_WITH_COMPLETED_PROVIDER_SHA256
