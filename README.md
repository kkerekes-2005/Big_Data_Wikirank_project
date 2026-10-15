# Big_Data_Wikirank_project
**Integrantes del equipo:** Kinga Kerekes, Laura Sánchez y Raúl Pérez

## 1. Descripción del proyecto
Este proyecto tiene como objetivo diseñar e implementar una arquitectura Big Data local para almacenar, procesar y analizar datos de Wikipedia utilizando Hadoop, HDFS, Spark y Docker.

Se utiliza el dataset WikiRank, que contiene aproximadamente 47 millones de artículos de Wikipedia en 55 idiomas, con un tamaño de 8,2 GB.

A partir de estos datos se realizan operaciones de limpieza, consultas, comparaciones de rendimiento y el entrenamiento de un modelo de Machine Learning para predecir la puntuación WikiRank de los artículos.

## 2. Arquitectura y tecnologías
El entorno se ejecuta mediante Docker Compose, permitiendo iniciar todos los servicios con un único comando.

Los componentes principales son:  
&emsp;**· HDFS:** almacenamiento distribuido mediante un NameNode y dos DataNodes, con factor de replicación 2.  
&emsp;**·YARN:** gestión de recursos y ejecución de trabajos MapReduce.  
&emsp;**· Apache Spark:** procesamiento distribuido de datos mediante un Spark Master y un Spark Worker.  
&emsp;**· Jupyter Notebook:** entorno de desarrollo y ejecución del código PySpark.  
&emsp;**· Hadoop Streaming:** ejecución de MapReduce mediante scripts Python.

## 3. Requisitos
Para ejecutar el proyecto se necesita:  
&emsp;· Docker Desktop instalado.  
&emsp;· Alrededor de 8 GB de memoria RAM disponibles para Docker.  
&emsp;· Aproximadamente 25 GB de espacio libre en disco.  
&emsp;· El dataset **wikirank_20240801.csv**.

**Dataset:** [https://huggingface.co/datasets/lewoniewski/wikipedia_quality_wikirank/viewer/default/wikirank_20240801?p=283499](https://huggingface.co/datasets/lewoniewski/wikipedia_quality_wikirank/viewer/default/wikirank_20240801?p=283499)  

El CSV no se incluye en el repositorio de GitHub debido a su tamaño.

## 4. Instalación y configuración
&emsp;1. Clonar o descargar este repositorio de GitHub.  
&emsp;2. Descargar el archivo **wikirank_20240801.csv** y guardarlo en la carpeta **data/**.  
&emsp;3. Abrir una terminal en la carpeta principal del proyecto.  
&emsp;4. Ejecutar `docker compose up -d` para iniciar los servicios.  
&emsp;5. Instalar Python 3 en el contenedor NodeManager, necesario para ejecutar Hadoop Streaming, siguiendo la configuración del proyecto.  

Una vez iniciado el entorno, se puede acceder a las siguientes interfaces:  
| Servicio | Dirección |
|---|---|
| HDFS NameNode | http://localhost:9870 |
| YARN ResourceManager | http://localhost:8088 |
| Spark Master | http://localhost:8080 |
| Jupyter Notebook | http://localhost:8888 |  

El token de acceso a Jupyter es **bigdata**.

## 5. Ejecución del proyecto
El punto de entrada principal es **notebooks/pipeline.ipynb**, que ejecuta el procesamiento completo de los datos.

El pipeline incluye:  
&emsp;1. Ingesta del CSV y almacenamiento en HDFS.  
&emsp;2. Comprobaciones de calidad y limpieza de los datos.  
&emsp;3. Conversión a formato Parquet y particionado por idioma.  
&emsp;4. Ejecución de consultas mediante Spark.  
&emsp;5. Entrenamiento y evaluación de un modelo de Machine Learning.  
&emsp;6. Almacenamiento de los resultados en HDFS.

El notebook puede ejecutarse desde Jupyter, seleccionando la opción de reiniciar el Kernel y ejecutar todas las celdas.

Además el proyecto incluye:  
&emsp;**· notebooks/experimentos.ipynb:** comparaciones de rendimiento entre RDDs y MapReduce, CSV y Parquet, optimización mediante broadcast join y análisis de data skew.  
&emsp;**· mapreduce/mapper.py y mapreduce/reducer.py:** implementación de MapReduce mediante Hadoop Streaming.  
&emsp;**· config.yaml:** configuración de rutas y parámetros del proyecto.

## 6. Organización de los datos en HDFS
Los datos se organizan en tres zonas:  
&emsp;**· /raw/wikirank:** almacena el CSV original sin modificaciones.  
&emsp;**· /curated/articles:** contiene los datos limpios en formato Parquet, particionados por idioma.  
&emsp;**· /curated/language_info:** contiene una tabla auxiliar con información de los idiomas.  
&emsp;**· /results:** almacena los resultados de las consultas, los informes de calidad y las métricas del modelo.

Esta organización permite conservar los datos originales y trabajar con versiones procesadas de forma más eficiente.

## 7. Estructura del repositorio
&emsp;**· docker-compose.yml:** definición de los servicios Docker.  
&emsp;**· hadoop.env:** configuración de Hadoop.  
&emsp;**· config.yaml:** parámetros del proyecto.  
&emsp;**· notebooks/:** notebooks del pipeline y experimentos.    
&emsp;**· mapreduce/:** scripts Python de MapReduce.  
&emsp;**· evidencias/:** capturas y resultados de las pruebas.  
&emsp;**· README.md:** documentación del proyecto.

## 8. Reproducibilidad
El proyecto está diseñado para poder ejecutarse de nuevo sin duplicar los datos ni alterar los resultados existentes.

Se utiliza el modo **overwrite** para las escrituras y se comprueba si el CSV ya existe en HDFS antes de realizar una nueva ingesta.

Los parámetros principales se encuentran en **config.yaml**, por lo que pueden modificarse sin cambiar directamente el código del pipeline.
