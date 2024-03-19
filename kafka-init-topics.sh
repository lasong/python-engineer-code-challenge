#!/bin/bash
# kafka-init-topics.sh
sleep 15

# Create topics
kafka-topics.sh --create --topic --if-not-exists data --bootstrap-server kafka:9092
kafka-topics.sh --create --topic --if-not-exists agg --bootstrap-server kafka:9092

echo "Topics created."
