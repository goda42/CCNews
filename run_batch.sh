#!/bin/bash
PY_SCRP=./src/newscrawl.py
SPARK_MASTER=$(head -1 spark-ip.txt)

spark-submit --master spark://$SPARK_MASTER:7077 $PY_SCRP
