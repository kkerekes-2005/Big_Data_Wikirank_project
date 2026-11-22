# Entrega 2: WikiRank de local a la nube (Databricks)

**Equipo:** Kinga Kerekes, Laura Sánchez y Raúl Pérez
**Asignatura:** Big Data & Cloud Computing, Universidad de Navarra, curso 2026-27

La Entrega 1 procesaba el dataset completo de WikiRank (47.019.883 artículos, 55 lenguas, 8,2 GB) en un clúster local de Docker con HDFS, YARN y Spark. Esta entrega traslada el mismo pipeline a **Databricks Free Edition (serverless)**. Mantiene las tres preguntas de negocio y compara los dos entornos: almacenamiento, particionado, planes de ejecución, tiempos y responsabilidades de administración.

La pregunta que guía el trabajo es qué cambia cuando se deja de configurar y administrar un clúster propio de Spark para ejecutar una solución equivalente en una plataforma gestionada.

---

## 1. Contenido de la carpeta

| Ruta | Qué es |
|---|---|
| `seleccion_datos.py` | Script que reduce el CSV original a un subconjunto de 1,89 GB |
| `wikirank_subset_metadata.json` | Metadatos generados por el script: fracción de cada lengua y recuentos originales. El notebook los necesita |
| `databricks/wikirank_databricks.ipynb` | Notebook de Databricks exportado con las salidas de la ejecución |
| `informe/Proyecto_Databricks_WikiRank_de_local_a_la_nube.pdf` | Informe de la entrega (12 páginas) |
| `presentacion/WikiRank_local_vs_nube.pptx` | Presentación |

El CSV del subconjunto (`wikirank_subset.csv`, 1,89 GB) **no está en el repositorio**, porque supera el límite de 100 MB de GitHub. Se regenera con el script (apartado 2).

---

## 2. Selección de datos: `seleccion_datos.py`

Databricks Free Edition admite ficheros de hasta 5 GB, y el enunciado limita el subconjunto a **2 GB**. El script reduce el CSV de 8,2 GB a **10.909.814 filas (1,89 GB)**. Solo quita filas: las 6 columnas del CSV original se conservan sin modificar.

### 2.1 Cómo se eligen las filas

El muestreo está estratificado por lengua, y cada parte de la regla responde a una de las preguntas de la Entrega 1:

- **Fracción común f en todas las lenguas** (f = 0,232 para pasar de 8,2 GB a 1,9 GB).
  - **P1:** la media por lengua no está sesgada, y el número de artículos se estima como `n_muestra / f_lengua`. Así se conserva la proporción entre lenguas.
  - **P3:** las correlaciones log(hablantes) ~ log(artículos) no cambian, porque dividir los recuentos por f solo desplaza log(artículos) en una constante. Las medias y los percentiles por escritura, familia y categoría FSI tampoco están sesgados.
- **Mínimo garantizado por lengua** (`--min-por-lengua`, 10.000 por defecto). Una lengua con pocos artículos recibe una fracción mayor hasta llegar al mínimo, o todas sus filas si tiene menos. Con 10.000 filas, el intervalo de confianza del 95 % de la media de P1 es de unos ±0,27 puntos, con una desviación típica de la puntuación de unos 13,8. Con f = 0,232, esta regla solo afecta al volapük (`vo`), que recibe f = 0,2642.
- **P2:** se conservan **siempre** las 10 mejores filas de cada lengua, ordenadas por `WikiRank_score` descendente y `Page_ID` ascendente. Así el top 10 es idéntico al del dataset completo. De esas 550 filas, 119 habrían entrado por sorteo y 431 entran solo por esta regla.
- **Reproducible.** Cada fila recibe un número de sorteo

  ```
  u = CRC32("<semilla>|<lengua>|<Page_ID>") / 2^32
  ```

  y entra si `u ≤ f_lengua`. El resultado no depende del orden del fichero. Este era el problema de `sample()` visto en la Entrega 1 (apartado 6.3): dio 2.349.961 y 2.352.462 filas en dos ejecuciones con la misma semilla. Spark tiene la misma función (`F.crc32`), así que el notebook puede recalcular `u` y saber por qué entró cada fila.

Una fila entra si cumple **una de las dos** condiciones: estar en el top 10 de su lengua, o tener `u ≤ f_lengua`.

### 2.2 Cómo funciona: dos lecturas en streaming

El CSV no se carga en memoria. Se recorre dos veces:

1. **Primera lectura (416 s):** no escribe nada. Por lengua, calcula los bytes, el número de filas, la media, el top 10 y el umbral del mínimo. Con eso fija la fracción f.
2. **Segunda lectura (204 s):** decide fila por fila y escribe el subconjunto. Usa un nombre temporal y lo renombra al final, para no dejar un fichero a medias si se interrumpe.

Además genera un **JSON de metadatos** con las fracciones por lengua y los recuentos originales. El notebook lo usa para estimar los totales y validar los resultados.

### 2.3 Uso

Solo usa la biblioteca estándar de Python (3.8 o superior).

```bash
python seleccion_datos.py --entrada data/wikirank_20240801.csv \
                          --salida  data/wikirank_subset.csv

# Opciones
python seleccion_datos.py ... --min-por-lengua 20000    # otro mínimo por lengua
python seleccion_datos.py ... --max-bytes 1500000000    # otro tamaño objetivo (por defecto 1.900.000.000)
python seleccion_datos.py ... --semilla 42              # sal del hash (por defecto "42")
python seleccion_datos.py ... --metadatos ruta.json     # por defecto: <salida>_metadata.json
```

| | Original | Subconjunto |
|---|---|---|
| Filas | 47.019.883 | 10.909.814 |
| Tamaño | 8,2 GB | 1,89 GB (1.890.380.375 bytes) |
| Columnas | 6 | 6, sin cambios |
| Lenguas | 55 | 55 |

---

## 3. Pipeline en Databricks

### 3.1 Arquitectura medallion

| Capa | Tablas (`workspace.wikirank.*`) | Equivalente en local |
|---|---|---|
| **Raw** | Volume `raw`: `wikirank_subset.csv` y `wikirank_subset_metadata.json` | Zona raw de HDFS |
| **Bronze** | `bronze_articles`: el CSV tal cual en Delta, más `_source_file` y `_ingested_at` | (no existía; en local se leía siempre el CSV) |
| **Silver** | `silver_articles`: limpia y particionada por `Language`, con las columnas de muestreo<br>`silver_quarantine_articles`: filas que no pasan los controles<br>`silver_language_info`: hablantes, escritura, familia y FSI de cada lengua<br>`silver_sampling_fractions`: fracciones del JSON | Zona curada (Parquet en HDFS) |
| **Gold** | `gold_q1_stats_by_language`, `gold_q2_top10_by_language`, `gold_q3_speakers_by_language`, `gold_q3_speakers`, `gold_q3_by_language_traits`, `gold_quality_report`, `gold_timings` | Resultados en HDFS |

Todas son tablas Delta registradas en Unity Catalog. Cada ejecución del notebook crea una versión nueva, que se puede consultar con `DESCRIBE HISTORY`, `VERSION AS OF` o `RESTORE`.

### 3.2 Columnas de muestreo en `silver_articles`

El CSV recortado no indica por qué entró cada fila. Silver lo reconstruye sin eliminar ninguna fila:

| Columna | Qué es |
|---|---|
| `f_lang` | Fracción de muestreo de la lengua (0,232, o 0,2642 en el volapük) |
| `u` | Número de sorteo, recalculado con `F.crc32` igual que el script |
| `en_muestra` | `u ≤ f_lang`: entró por sorteo |
| `en_muestra_comun` | `u ≤ 0,232`: habría entrado con la fracción común |

Cada pregunta usa las filas que corresponden a lo que mide:

| Pregunta | Filas que usa |
|---|---|
| P1: estadísticas por lengua | `en_muestra`. El total se estima como `COUNT_IF(en_muestra) / f_lang` |
| P2: top 10 por lengua | Todas |
| P3a: hablantes frente a tamaño y calidad | Resultados de P1 |
| P3b: distribución por escritura, familia y FSI | `en_muestra_comun`, para que todas las lenguas pesen igual |

La regla se fijó en el script antes de cualquier análisis y no mira el resultado. Separar las 431 filas forzadas evita que suban las medias y los percentiles de P1 y P3. Una celda de control comprueba que toda fila con `en_muestra` falso pertenece al top 10. Resultado: **0 filas sin explicación**.

### 3.3 Secciones del notebook

| Sección | Contenido |
|---|---|
| Configuración | Equivalente a `config.yaml`. Funciones `T()` (nombre de tabla por capa), `save()` (escritura Delta) y `timed()` (tiempos por etapa) |
| 0. Entorno | Qué propiedades de Spark se pueden fijar en serverless. Solo `shuffle.partitions` y `session.timeZone`; el resto devuelve `CONFIG_NOT_AVAILABLE` |
| 1. Ingesta | Comprobación del volume, particiones de lectura (15) y capa bronze |
| 2. Calidad | Controles (nulos, rangos, duplicados) y cuarentena |
| 3. Silver | Limpieza, columnas de muestreo y comprobación del hash |
| 4. `language_info` | Tabla auxiliar de idiomas (`langcodes`) |
| 5. Gold | P1, P2, P3a y P3b en Spark SQL, más la comparación de P1 con los resultados locales |
| 6. Planes | `explain("extended")` y `explain("formatted")` de P1, P2 y P3b |
| 7. Delta | `DESCRIBE HISTORY`, `DESCRIBE DETAIL` y `VERSION AS OF` |
| 8. Tiempos | Resumen de `timed()` |

Las preguntas de negocio están en SQL y la ingesta y la limpieza en DataFrames. El rendimiento es el mismo, porque las dos formas pasan por Catalyst. Las consultas SQL se pueden ejecutar además en un SQL warehouse o en un panel sin Python.

---

## 4. Cómo reproducirlo

1. **Generar el subconjunto** en local con `seleccion_datos.py` (apartado 2.3). Tarda unos 10 minutos.
2. **Crear el volume** en Databricks Free Edition: *Catalog → workspace → Create schema `wikirank` → Create volume `raw`*. El notebook también lo crea si no existe.
3. **Subir los dos ficheros** al volume: `wikirank_subset.csv` y `wikirank_subset_metadata.json`, en `/Volumes/workspace/wikirank/raw/`.
4. **Importar el notebook**: *Workspace → Import → `databricks/wikirank_databricks.ipynb`*.
5. **Conectarlo a cómputo serverless** y ejecutar *Run all*. El notebook instala `langcodes==3.5.1` y `language_data==1.4.0`.
6. **Revisar los resultados** en *Catalog → workspace → wikirank* (tablas bronze, silver y gold) o desde el editor SQL.

El notebook es idempotente: al volver a ejecutarlo, las tablas se sobrescriben y cada una gana una versión en su historial Delta.

---

## 5. Resultados frente al clúster local

| | Local (Entrega 1, dataset completo) | Databricks (subconjunto del 23 %) |
|---|---|---|
| P1: total de artículos | 47.019.883 | 47.018.319 (estimado, −0,003 %) |
| P1: error máximo | — | Recuento 0,95 % (vo); media 0,14 puntos (sl) |
| P2: top 10 | — | Idéntico en las 55 lenguas |
| P3a: r(log hablantes, log artículos) | 0,566 | 0,566 |
| P3a: r(log hablantes, media) | 0,106 | 0,105 |

| Etapa (s) | Local (8,2 GB, 2 núcleos) | Databricks (1,89 GB), 2.ª ejecución |
|---|---|---|
| Controles de calidad | 122 | 2,3 |
| Escritura curada / silver | 118 | 11,6 |
| P2 | 42,0 | 2,9 |
| P3b | 93,0 | 3,4 |

Los tiempos no son directamente comparables: Databricks procesa la cuarta parte de los datos, con otro hardware y con Photon. La comparación detallada de planes físicos, particionado, shuffles y optimizaciones está en el informe (apartados 4 a 6).

---

## 6. Principales diferencias entre los dos entornos

1. **Quién administra la infraestructura.** En local: 9 contenedores, esperar al modo seguro de HDFS y ajustar la memoria de los executors. En Databricks: el cómputo serverless arranca en segundos, a cambio de depender de los límites del proveedor (5 GB por fichero, sin RDD ni `sparkContext`).
2. **Almacenamiento.** Se pasa de ficheros en HDFS con replicación 2 a tablas Delta gobernadas en Unity Catalog, con historial de versiones, linaje y `RESTORE`.
3. **Control frente a comodidad.** En local se podía configurar todo y usar MapReduce y RDD. En Databricks optimizan Photon y AQE automáticamente, y solo 2 de 7 propiedades de Spark son configurables.

---

## 7. Entregables

- [x] Script de selección de datos: `seleccion_datos.py`
- [x] Notebook de Databricks: `databricks/wikirank_databricks.ipynb`
- [x] Informe (máximo 12 páginas): `informe/`
- [x] Presentación: `presentacion/`
- [ ] Workspace de Databricks compartido con los dos docentes
