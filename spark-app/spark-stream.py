#!/usr/bin/env python3
from spark_crime import query_crime

print("✅ Streaming initialisé : lecture depuis 'send-data', écriture vers 'train-data'.")
query_crime.awaitTermination()