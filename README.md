# Big_Data_Wikirank_project
**Integrantes del equipo:** Kinga Kerekes, Laura Sánchez y Raúl Pérez\
**Asignatura:** Big Data & Cloud Computing, Universidad de Navarra, curso 2026-27

## 1. Descripción del proyecto
Este proyecto almacena, procesa y analiza el dataset WikiRank: unos 47 millones de artículos de Wikipedia en 55 idiomas, con un tamaño de 8,2 GB. Responde a tres preguntas de negocio:

- **P1:** cuántos artículos tiene cada idioma y cuál es su puntuación WikiRank media.
- **P2:** cuáles son los 10 mejores artículos de cada idioma.
- **P3:** cómo se relacionan el número de hablantes, la escritura, la familia lingüística y la dificultad (FSI) con el tamaño y la calidad de cada Wikipedia.

El proyecto tiene dos entregas:

| Entrega | Entorno | Dónde está |
|---|---|---|
| **1. Clúster local** | Arquitectura Big Data propia con Docker, HDFS, YARN y Spark. Limpieza, consultas, MapReduce, experimentos de rendimiento y un modelo de Machine Learning para predecir la puntuación | Raíz del repositorio; apartados 2 a 8 de este README |
| **2. De local a la nube** | El mismo pipeline en Databricks Free Edition (serverless, Delta Lake, arquitectura medallion), con un subconjunto de 1,89 GB, y la comparación entre los dos entornos | Carpeta [`entrega2/`](entrega2/); resumen en el apartado 9 |

> Los apartados 2 a 8 describen la **Entrega 1**. La Entrega 2 tiene su propio README en [`entrega2/README.md`](entrega2/README.md).

## 2. Arquitectura y tecnologías
El entorno se ejecuta mediante Docker Compose, permitiendo iniciar todos los servicios con un único comando.

Los componentes principales son:

- **HDFS:** almacenamiento distribuido mediante un NameNode y dos DataNodes, con factor de replicación 2.
- **YARN:** gestión de recursos y ejecución de trabajos MapReduce.
- **Apache Spark:** procesamiento distribuido de datos mediante un Spark Master y un Spark Worker.
- **Jupyter Notebook:** entorno de desarrollo y ejecución del código PySpark.
- **Hadoop Streaming:** ejecución de MapReduce mediante scripts Python.

### Recursos asignados
| Servicio | Recursos | Dónde se configura |
|---|---|---|
| Spark worker | 2 núcleos y 3 GB | `docker-compose.yml` |
| Executor de Spark | 3 GB (`spark.executor_memory`) | `config.yaml` |
| YARN NodeManager | 16 GB y 8 vcores | `hadoop.env` |
| Contenedores de MapReduce | map 4 GB, reduce 8 GB | `hadoop.env` |
| HDFS | 2 DataNodes, replicación 2, bloques de 128 MB (valor por defecto) | `docker-compose.yml`, `hadoop.env` |

## 3. Requisitos
Para ejecutar el proyecto se necesita:

- Docker Desktop instalado.
- Alrededor de 8 GB de memoria RAM disponibles para Docker: se recomienda un equipo de 16 GB. En uno de 8 GB funciona, pero hay que iniciar Docker en limpio (`wsl --shutdown` y volver a abrir Docker Desktop) y cerrar otras aplicaciones; si no, Spark puede perder executors (`exit code 137`).
- Aproximadamente 35 GB de espacio libre en disco.
- El dataset **wikirank_20240801.csv**.

### Dataset

WikiRank (Lewoniewski, 2024): https://huggingface.co/datasets/lewoniewski/wikipedia_quality_wikirank

Descargar el fichero `wikirank_20240801.csv` (8,2 GB) y crear la carpeta `data/` en la raíz del proyecto y copiar ahí el CSV.
El CSV no se incluye en el repositorio de GitHub debido a su tamaño (supera el límite 100MB).

## 4. Instalación y configuración
1. Clonar o descargar este repositorio de GitHub.
2. Descargar el dataset y guardarlo en **data/** (ver apartado *Dataset*).
3. Abrir una terminal en la carpeta principal del proyecto.

### Arrancar el entorno

```bash
docker compose up -d
docker compose logs -f jupyter
```

En los logs aparece un enlace del tipo `http://127.0.0.1:8888/lab?token=bigdata`: ábrelo en el navegador
(Ctrl+C para salir de los logs; los contenedores siguen funcionando).

Antes de ejecutar nada, esperar a que HDFS salga del modo seguro (`docker exec namenode hdfs dfsadmin -safemode get` debe responder `Safe mode is OFF`).

Para parar el entorno sin perder los datos: `docker compose down`. No usar `docker compose down -v`, que borra todo el contenido de HDFS.

Interfaces web:
- NameNode (HDFS): http://localhost:9870
- Spark master: http://localhost:8080
- Spark UI (mientras hay una sesión abierta): http://localhost:4040
- YARN: http://localhost:8088

### Python 3 en el NodeManager (solo para MapReduce)

El contenedor del NodeManager no incluye Python, necesario para Hadoop Streaming. Se instala una vez:

```bash
docker exec -it nodemanager bash -c "sed -i 's/deb.debian.org/archive.debian.org/g; s/security.debian.org/archive.debian.org/g; /stretch-updates/d' /etc/apt/sources.list && apt-get update && apt-get install -y python3"
```

## 5. Ejecución del proyecto

### Un solo comando (recomendado)
Con el CSV en `data/`, desde la carpeta principal del proyecto:

```powershell
# Windows (PowerShell)
powershell -ExecutionPolicy Bypass -File run_all.ps1
```
```bash
# Linux / macOS / Git Bash
bash run_all.sh
```

El script levanta el entorno, espera a que HDFS salga del modo seguro, instala Python 3 en el NodeManager si falta y ejecuta en orden: pipeline (ingesta → HDFS → Spark → resultados → modelo), job de MapReduce y experimentos. Se detiene si algún paso falla. Los notebooks ejecutados se guardan en `salidas/`. Duración total ≈ 30 min en un PC del aula.

### Paso a paso (alternativa)

El punto de entrada principal es **notebooks/pipeline.ipynb**, que ejecuta el procesamiento completo de los datos.

El pipeline incluye:

1. Ingesta del CSV y almacenamiento en HDFS.
2. Comprobaciones de calidad, cuarentena de filas inválidas y limpieza de los datos.
3. Conversión a formato Parquet y particionado por idioma.
4. Ejecución de consultas mediante Spark.
5. Entrenamiento y evaluación de un modelo de Machine Learning.
6. Almacenamiento de los resultados en HDFS.

**Opción A, desde Jupyter:** abrir `notebooks/pipeline.ipynb` y ejecutar *Restart Kernel and Run All Cells*.

**Opción B, sin abrir Jupyter:**

```bash
docker exec jupyter bash -c 'export SPARK_HOME=${SPARK_HOME:-/usr/local/spark}; export PYTHONPATH=$SPARK_HOME/python:$(ls $SPARK_HOME/python/lib/py4j-*-src.zip):$PYTHONPATH; jupyter nbconvert --to notebook --execute --inplace --ExecutePreprocessor.timeout=-1 /home/jovyan/work/notebooks/pipeline.ipynb'
```
(`docker exec` no carga el entorno de Spark de la imagen; por eso se define `PYTHONPATH` antes de `nbconvert`.)

Después del pipeline se ejecuta **notebooks/experiments.ipynb** de la misma forma (lee la zona curada):
comparaciones de rendimiento con RDDs, CSV frente a Parquet, optimización mediante broadcast join y análisis de data skew.

### MapReduce (Hadoop Streaming)
Requiere Python 3 en el NodeManager (apartado 4) y el CSV ya cargado en `/raw/wikirank` por el pipeline.

```bash
docker exec -it namenode hdfs dfs -rm -r -f /results/mr_stats_by_language
docker exec -it namenode bash -c "time hadoop jar /opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar -files /mapreduce/mapper.py,/mapreduce/reducer.py -mapper 'python3 mapper.py' -reducer 'python3 reducer.py' -input /raw/wikirank -output /results/mr_stats_by_language"
```
La primera línea borra la salida anterior (Hadoop no escribe en una carpeta que ya existe), así que el job es repetible. El resultado queda en `/results/mr_stats_by_language`; `experiments.ipynb` lo compara con P1.

### Resultados esperados
| Paso | Resultado |
|---|---|
| Ingesta | `/raw/wikirank/wikirank_20240801.csv`, 8.195.179.141 bytes, 62 bloques, replicación 2 |
| Calidad | 47.019.883 filas; 0 nulos, 0 fuera de rango, 0 duplicados, 0 filas en cuarentena |
| Zona curada | 47.019.883 filas en 55 carpetas `Language=xx` (un Parquet por lengua) |
| P1 | 55 lenguas; inglés 6.860.430 artículos, media 24,71 |
| P3 | r(log hablantes, log artículos) = 0,566; r(log hablantes, media) = 0,106 |
| Modelo | RMSE ≈ 13,2 frente a 13,8 de la predicción constante (R² ≈ 0,08) |
| MapReduce | 55 líneas (lengua, artículos, media), idénticas a P1 |

Duración aproximada: pipeline ≈ 13 min (≈ 15 la primera vez), MapReduce ≈ 4 min, experimentos ≈ 10 min. Los tiempos varían según el equipo, y la muestra del modelo puede variar ligeramente entre ejecuciones.

## 6. Organización de los datos en HDFS
Los datos se organizan en cuatro zonas:

- **Raw (/raw/wikirank):** el CSV original sin modificaciones.
- **Curated:** datos limpios en Parquet. **/curated/articles** (particionado por idioma), **/curated/language_info** (tabla auxiliar de idiomas) y **/curated/title_words** (frecuencia de las palabras de los títulos, para el modelo).
- **Quarantine (/quarantine/articles):** filas que no superan los controles de calidad, guardadas para revisarlas.
- **Results (/results):** resultados de las consultas, informe de calidad y métricas del modelo.

Esta organización permite conservar los datos originales y trabajar con versiones procesadas de forma más eficiente.

## 7. Estructura del repositorio
**Entrega 1 (raíz):**

- **docker-compose.yml:** definición de los servicios Docker.
- **hadoop.env:** configuración de Hadoop.
- **config.yaml:** parámetros del proyecto.
- **notebooks/:** notebooks del pipeline y experimentos.
- **mapreduce/:** scripts Python de MapReduce.
- **evidencias/:** capturas y resultados de las pruebas.
- **run_all.ps1 / run_all.sh:** ejecución completa con un solo comando.

**Entrega 2 ([`entrega2/`](entrega2/)):**

- **seleccion_datos.py:** script que reduce el CSV de 8,2 GB a un subconjunto de 1,89 GB.
- **wikirank_subset_metadata.json:** fracciones de muestreo por lengua, que usa el notebook.
- **databricks/:** notebook de Databricks exportado con las salidas.
- **informe/:** informe de la entrega en PDF.
- **presentacion/:** presentación.
- **README.md:** documentación de la Entrega 2.

## 8. Reproducibilidad
El proyecto está diseñado para poder ejecutarse de nuevo sin duplicar los datos ni alterar los resultados existentes.

Se utiliza el modo **overwrite** para las escrituras y se comprueba si el CSV ya está en HDFS con el tamaño esperado antes de realizar una nueva ingesta.

Los parámetros principales se encuentran en **config.yaml**, por lo que pueden modificarse sin cambiar directamente el código del pipeline.

Se comprobó ejecutando el pipeline dos veces (`evidencias/pipeline_1st_pass.ipynb` frente a `notebooks/pipeline.ipynb`, e `idempotencia_run1.txt` frente a `idempotencia_run2.txt`): la segunda ejecución no vuelve a subir el CSV, y la zona curada y los resultados de P1–P3 son idénticos.

## 9. Entrega 2: de local a la nube (Databricks)
La Entrega 2 traslada el pipeline a **Databricks Free Edition**. Mantiene las tres preguntas de negocio y compara los dos entornos. Todo el detalle está en [`entrega2/README.md`](entrega2/README.md) y en el informe.

**Selección de datos.** Databricks Free Edition limita la subida a 5 GB por fichero, y el enunciado, a 2 GB. `entrega2/seleccion_datos.py` reduce el CSV a 10.909.814 filas (1,89 GB) sin tocar las columnas:

- Muestreo estratificado del 23,2 % por lengua, reproducible con un hash CRC32.
- Mínimo de 10.000 filas por lengua.
- El top 10 de cada lengua siempre incluido.

**Pipeline.** Arquitectura medallion con tablas Delta en Unity Catalog:

- **Raw:** volume con el CSV.
- **Bronze:** CSV en Delta.
- **Silver:** datos limpios, particionados por lengua y con columnas de muestreo.
- **Gold:** P1, P2 y P3 en Spark SQL.

**Resultados frente a la Entrega 1:**

| | Local (dataset completo) | Databricks (subconjunto) |
|---|---|---|
| P1: total de artículos | 47.019.883 | 47.018.319 (estimado, −0,003 %) |
| P2: top 10 | — | Idéntico en las 55 lenguas |
| P3: r(log hablantes, log artículos) | 0,566 | 0,566 |

**Qué cambia entre los dos entornos:**

1. **Quién administra la infraestructura.** En la Entrega 1 había que levantar 9 contenedores, esperar a que HDFS saliera del modo seguro y ajustar la memoria de los executors. En Databricks, el cómputo serverless arranca en segundos, a cambio de depender de los límites del proveedor.
2. **Almacenamiento.** Se pasa de ficheros en HDFS con replicación 2 a tablas Delta gobernadas, con historial de versiones y `RESTORE`.
3. **Control frente a comodidad.** En local se podía configurar todo; en Databricks optimizan Photon y AQE automáticamente, y solo 2 de las 7 propiedades de Spark probadas son configurables.
