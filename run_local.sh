#!/usr/bin/env bash
set -e

echo "Building project..."
sbt clean package

echo "Running Spark job..."
spark-submit \
  --packages com.typesafe:config:1.4.3 \
  --class omnichain.app.Main \
  --master local[*] \
  target/scala-2.13/omnichain_2.13-0.1.0.jar
