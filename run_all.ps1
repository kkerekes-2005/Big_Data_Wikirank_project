# Ejecuta el proyecto completo con un solo comando (Windows / PowerShell):
#   powershell -ExecutionPolicy Bypass -File run_all.ps1
# Orden: entorno -> pipeline (ingesta, HDFS, Spark, resultados, ML) -> MapReduce -> experimentos.
# Los notebooks ejecutados se guardan en salidas/ (los de notebooks/ no se modifican).
# docker exec no carga el entorno de Spark de la imagen, por eso se define PYTHONPATH antes de nbconvert.

$t0 = Get-Date

function Check($msg) {
    if ($LASTEXITCODE -ne 0) { Write-Host "ERROR en: $msg" -ForegroundColor Red; exit 1 }
}
function Step($msg) { Write-Host "`n=== $msg ===" -ForegroundColor Cyan }

Step "1/6 Levantar el entorno"
docker compose up -d
Check "docker compose up"

Step "2/6 Esperar a que HDFS salga del modo seguro"
$ok = $false
for ($i = 0; $i -lt 60; $i++) {
    $sm = (docker exec namenode hdfs dfsadmin -safemode get) | Out-String
    if ($sm -match "OFF") { $ok = $true; break }
    Start-Sleep -Seconds 5
}
if (-not $ok) { Write-Host "ERROR: HDFS sigue en modo seguro tras 5 minutos" -ForegroundColor Red; exit 1 }
Write-Host "Safe mode is OFF"

Step "3/6 Python 3 en el NodeManager (solo se instala si falta)"
docker exec nodemanager bash -c "command -v python3 >/dev/null || (sed -i 's/deb.debian.org/archive.debian.org/g; s/security.debian.org/archive.debian.org/g; /stretch-updates/d' /etc/apt/sources.list && apt-get update && apt-get install -y python3)"
Check "instalar python3"

Step "4/6 Pipeline (notebooks/pipeline.ipynb)"
docker exec jupyter bash -c 'export SPARK_HOME=${SPARK_HOME:-/usr/local/spark}; export PYTHONPATH=$SPARK_HOME/python:$(ls $SPARK_HOME/python/lib/py4j-*-src.zip):$PYTHONPATH; jupyter nbconvert --to notebook --execute --ExecutePreprocessor.timeout=-1 --output-dir /home/jovyan/work/salidas /home/jovyan/work/notebooks/pipeline.ipynb'
Check "pipeline.ipynb"

Step "5/6 MapReduce (Hadoop Streaming)"
docker exec namenode hdfs dfs -rm -r -f /results/mr_stats_by_language
docker exec namenode bash -c "time hadoop jar /opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar -files /mapreduce/mapper.py,/mapreduce/reducer.py -mapper 'python3 mapper.py' -reducer 'python3 reducer.py' -input /raw/wikirank -output /results/mr_stats_by_language"
Check "MapReduce"

Step "6/6 Experimentos (notebooks/experiments.ipynb)"
docker exec jupyter bash -c 'export SPARK_HOME=${SPARK_HOME:-/usr/local/spark}; export PYTHONPATH=$SPARK_HOME/python:$(ls $SPARK_HOME/python/lib/py4j-*-src.zip):$PYTHONPATH; jupyter nbconvert --to notebook --execute --ExecutePreprocessor.timeout=-1 --output-dir /home/jovyan/work/salidas /home/jovyan/work/notebooks/experiments.ipynb'
Check "experiments.ipynb"

$min = [math]::Round(((Get-Date) - $t0).TotalMinutes, 1)
Write-Host "`nListo en $min min. Notebooks ejecutados en salidas/, resultados en HDFS (/results)." -ForegroundColor Green
