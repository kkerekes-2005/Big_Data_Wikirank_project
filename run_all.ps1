$ErrorActionPreference = "Stop"

Write-Host "=== 1. Iniciando contenedores Docker ===" -ForegroundColor Green
docker compose up -d

Write-Host "=== 2. Esperando a que HDFS salga del modo seguro ===" -ForegroundColor Green
docker exec namenode hdfs dfsadmin -safemode wait

Write-Host "=== 3. Verificando Python 3 en el NodeManager (para MapReduce) ===" -ForegroundColor Green
docker exec nodemanager python3 --version 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Host "Python 3 no detectado en NodeManager. Instalando (solo 1ª vez)..." -ForegroundColor Yellow
    docker exec nodemanager bash -c "sed -i 's/deb.debian.org/archive.debian.org/g; s/security.debian.org/archive.debian.org/g; /stretch-updates/d' /etc/apt/sources.list && apt-get update && apt-get install -y python3"
} else {
    Write-Host "Python 3 en NodeManager listo. Continuando..." -ForegroundColor Cyan
}

Write-Host "=== 4. Verificando librerías en el contenedor Jupyter ===" -ForegroundColor Green
docker exec jupyter python -c "import pyspark, yaml, langcodes, language_data" 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Host "Faltan librerías en Jupyter. Instalando (solo 1ª vez)..." -ForegroundColor Yellow
    docker exec jupyter pip install --no-cache-dir pyspark pyyaml langcodes==3.5.1 language_data==1.4.0
} else {
    Write-Host "Librerías de Jupyter listas. Continuando..." -ForegroundColor Cyan
}

Write-Host "=== 5. Ejecutando Pipeline principal (pipeline.ipynb) ===" -ForegroundColor Green
docker exec jupyter jupyter nbconvert --to notebook --execute --inplace --ExecutePreprocessor.timeout=-1 /home/jovyan/work/notebooks/pipeline.ipynb

Write-Host "=== 6. Ejecutando trabajo de Hadoop MapReduce ===" -ForegroundColor Green
docker exec namenode hdfs dfs -rm -r -f /results/mr_stats_by_language
docker exec namenode bash -c "hadoop jar /opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar -files /mapreduce/mapper.py,/mapreduce/reducer.py -mapper 'python3 mapper.py' -reducer 'python3 reducer.py' -input /raw/wikirank -output /results/mr_stats_by_language"

Write-Host "=== 7. Ejecutando Experimentos (experiments.ipynb) ===" -ForegroundColor Green
docker exec jupyter jupyter nbconvert --to notebook --execute --inplace --ExecutePreprocessor.timeout=-1 /home/jovyan/work/notebooks/experiments.ipynb

Write-Host "=== 8. Deteniendo los contenedores ===" -ForegroundColor Green
docker compose stop

Write-Host "==================================================" -ForegroundColor Cyan
Write-Host "   ¡Ejecución del proyecto completada con éxito!  " -ForegroundColor Cyan
Write-Host "==================================================" -ForegroundColor Cyan