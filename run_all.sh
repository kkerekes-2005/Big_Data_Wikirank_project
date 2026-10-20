#!/usr/bin/env bash
# Ejecuta el proyecto completo con un solo comando (Linux / macOS / Git Bash):
#   bash run_all.sh
# Orden: entorno -> pipeline (ingesta, HDFS, Spark, resultados, ML) -> MapReduce -> experimentos.
# Los notebooks ejecutados se guardan en salidas/ (los de notebooks/ no se modifican).
# docker exec no carga el entorno de Spark de la imagen, por eso se define PYTHONPATH antes de nbconvert.
set -euo pipefail
SECONDS=0
step() { printf '\n=== %s ===\n' "$1"; }

step "1/6 Levantar el entorno"
docker compose up -d

step "2/6 Esperar a que HDFS salga del modo seguro"
for i in $(seq 60); do
  if docker exec namenode hdfs dfsadmin -safemode get 2>/dev/null | grep -q OFF; then echo "Safe mode is OFF"; break; fi
  if [ "$i" -eq 60 ]; then echo "ERROR: HDFS sigue en modo seguro tras 5 minutos"; exit 1; fi
  sleep 5
done

step "3/6 Python 3 en el NodeManager (solo se instala si falta)"
docker exec nodemanager bash -c "command -v python3 >/dev/null || (sed -i 's/deb.debian.org/archive.debian.org/g; s/security.debian.org/archive.debian.org/g; /stretch-updates/d' /etc/apt/sources.list && apt-get update && apt-get install -y python3)"

step "4/6 Pipeline (notebooks/pipeline.ipynb)"
docker exec jupyter bash -c 'export SPARK_HOME=${SPARK_HOME:-/usr/local/spark}; export PYTHONPATH=$SPARK_HOME/python:$(ls $SPARK_HOME/python/lib/py4j-*-src.zip):$PYTHONPATH; jupyter nbconvert --to notebook --execute --ExecutePreprocessor.timeout=-1 --output-dir /home/jovyan/work/salidas /home/jovyan/work/notebooks/pipeline.ipynb'

step "5/6 MapReduce (Hadoop Streaming)"
docker exec namenode hdfs dfs -rm -r -f /results/mr_stats_by_language
docker exec namenode bash -c "time hadoop jar /opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar -files /mapreduce/mapper.py,/mapreduce/reducer.py -mapper 'python3 mapper.py' -reducer 'python3 reducer.py' -input /raw/wikirank -output /results/mr_stats_by_language"

step "6/6 Experimentos (notebooks/experiments.ipynb)"
docker exec jupyter bash -c 'export SPARK_HOME=${SPARK_HOME:-/usr/local/spark}; export PYTHONPATH=$SPARK_HOME/python:$(ls $SPARK_HOME/python/lib/py4j-*-src.zip):$PYTHONPATH; jupyter nbconvert --to notebook --execute --ExecutePreprocessor.timeout=-1 --output-dir /home/jovyan/work/salidas /home/jovyan/work/notebooks/experiments.ipynb'

echo; echo "Listo en $((SECONDS / 60)) min. Notebooks ejecutados en salidas/, resultados en HDFS (/results)."
