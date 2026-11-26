# Reto 1: mi primer pipeline declarativo con Data Vault

**Nivel:** inicial · **Duración sugerida:** 1–2 horas  
**Modalidad:** individual o en parejas.

## Objetivo

Crear un Job que genere eventos ficticios y un pipeline de Lakeflow Spark Declarative Pipelines que los lleve por tres capas fijas: **Bronce** (`raw_data.bronze`), **Plata** modelada como **Data Vault** (`data_vault.plata`) y **Oro** (`ods.gold`). No se conecta a una fuente real y no creas catálogos, esquemas ni volúmenes.

## Resultado esperado

Con `lote_01`: Bronce 3 filas, hub 3, satélite 3; en Oro, 3 eventos, 2 aeronaves distintas y velocidad máxima de 300. Para el sufijo de ejemplo `dsh`, el grafo muestra `r01_vuelos_raw_dsh → r01_stg_vuelos_dsh → r01_hub_vuelo_dsh` y `r01_sat_vuelo_dsh → r01_indicadores_vuelos_dsh`.

## Arquitectura y tablas

| Capa | Objeto (sufijo `dsh`) | Tipo | Contenido |
|---|---|---|---|
| Aterrizaje | `/Volumes/raw_data/bronze/landing_retos_sdp/dsh/reto01/entrada` | Carpeta en volumen compartido | Archivos JSON del generador |
| Bronce | `raw_data.bronze.r01_vuelos_raw_dsh` | Streaming table (Auto Loader) | El dato tal como llega + `archivo_origen`, `fecha_archivo`, `fecha_ingesta` |
| Staging | `r01_stg_vuelos_dsh` | Vista temporal privada (no se publica) | Calcula hash key, hashdiff, `fecha_carga` y `fuente_registro` |
| Plata | `data_vault.plata.r01_hub_vuelo_dsh` | Streaming table | Hub: una fila por `evento_id` |
| Plata | `data_vault.plata.r01_sat_vuelo_dsh` | Streaming table | Satélite: aeronave, país, velocidad y persona del vuelo |
| Oro | `ods.gold.r01_indicadores_vuelos_dsh` | Vista materializada | Eventos, aeronaves distintas y velocidad máxima |

## Data Vault en pocas palabras

- **Hub:** la lista única de llaves de negocio, aquí `evento_id`. No guarda atributos descriptivos.
- **Satélite:** los atributos que describen la llave (aeronave, país, velocidad, persona). Se une al hub por la hash key.
- **Link:** la relación entre dos o más hubs. Aparece en el reto 3.
- **Hash key (`hk_vuelo`):** SHA-256 de la llave de negocio *normalizada* (sin espacios y en mayúsculas). Siempre da el mismo valor para la misma llave, sin importar quién la calcule.
- **Hashdiff:** SHA-256 de los atributos normalizados del satélite. Para saber si algo cambió basta comparar un solo valor.
- **`fecha_carga` y `fuente_registro`:** cuándo llegó el archivo a la landing y de qué archivo viene cada fila (trazabilidad).

En este reto los eventos son inmutables y llegan una sola vez, por eso hub y satélite son streaming tables que solo agregan filas. El reto 2 resuelve duplicados y cambios.

## Requisitos

- Workspace de Databricks con Unity Catalog, Lakeflow pipelines y Auto Loader habilitados, y cómputo compatible.
- No creas catálogos, esquemas ni volúmenes: el instructor ya preparó las capas fijas, el volumen compartido y los permisos (ver la sección siguiente).
- Acceso a SQL Editor o a un notebook y permiso para ejecutar el pipeline. El uso de cómputo y almacenamiento puede generar costos.
- Permisos para crear y ejecutar Lakeflow Jobs y para escribir en el volumen con la identidad que ejecuta el Job.
- Conocimientos básicos de Python y SQL. Los datos son sintéticos y no requieren una API externa.

## Preparación del instructor (una vez)

Los catálogos `raw_data`, `data_vault` y `ods` y los esquemas `raw_data.bronze`, `data_vault.plata` y `ods.gold` ya existen. El instructor crea el volumen compartido y otorga los permisos una sola vez; sirven para los tres retos. Sustituye `participantes` por el grupo de participantes (recomendado) o por un correo:

```sql
CREATE VOLUME IF NOT EXISTS raw_data.bronze.landing_retos_sdp;

GRANT USE CATALOG ON CATALOG raw_data   TO `participantes`;
GRANT USE CATALOG ON CATALOG data_vault TO `participantes`;
GRANT USE CATALOG ON CATALOG ods        TO `participantes`;

GRANT USE SCHEMA, CREATE TABLE, SELECT ON SCHEMA raw_data.bronze  TO `participantes`;
GRANT USE SCHEMA, CREATE TABLE, SELECT ON SCHEMA data_vault.plata TO `participantes`;
GRANT USE SCHEMA, CREATE MATERIALIZED VIEW, SELECT ON SCHEMA ods.gold TO `participantes`;

GRANT READ VOLUME, WRITE VOLUME ON VOLUME raw_data.bronze.landing_retos_sdp TO `participantes`;
```

**Advertencia: el volumen es compartido.** Con `WRITE VOLUME` cualquier participante puede leer, escribir y borrar archivos en cualquier carpeta del volumen, no solo en la suya. Cada persona debe trabajar únicamente en `landing_retos_sdp/<su_sufijo>/` y nunca borrar archivos ajenos. Usa solo datos dummy. Con `SELECT` en los esquemas, los participantes también pueden consultar las tablas de los demás; cada quien es dueño solo de las tablas que crea su pipeline. Los participantes necesitan además permiso para crear pipelines (serverless o con una política de clúster) y Jobs.

## Identificación por participante

Forma un sufijo con la primera letra del nombre y de cada uno de los dos apellidos, en minúsculas y sin tildes; por ejemplo, `dsh`. Usa tres letras ASCII (`a`–`z`). Si dos participantes tienen las mismas iniciales o una persona no puede formar tres letras, el instructor asignará a cada uno un sufijo alternativo de tres letras **único**. Confírmalo antes de crear recursos.

Todos los ejemplos usan `dsh`: sustitúyelo por tu sufijo en **cada** nombre, consulta y ruta. Todas las tablas llevan el prefijo del reto (`r01_`) y tu sufijo, por ejemplo `raw_data.bronze.r01_vuelos_raw_dsh`, para no chocar con otros retos ni con otras personas. Tu carpeta de aterrizaje es `/Volumes/raw_data/bronze/landing_retos_sdp/dsh/reto01/entrada`; el pipeline la deriva del sufijo. La separación por nombres no reemplaza permisos de Unity Catalog ni protege frente al acceso de otros usuarios.

Los pipelines existentes con nombres anteriores no se migran automáticamente: crea un pipeline nuevo con sus recursos propios; no cambies el sufijo en un pipeline compartido o ya ejecutado.

## Datos ficticios y Job

Los campos `persona_id` y `persona_alias` son identificadores y etiquetas inventados, como `DUMMY-ID-001` y `DUMMY_UCRANIA`; los nombres de países son solo alias, no representan personas reales. No se usan documentos, correos, direcciones ni otros datos personales reales. No cargues información real en este laboratorio.

1. Importa `ingesta/generador.py` como notebook de Databricks o selecciónalo desde una Git folder.
2. En **Jobs & Pipelines**, crea el Job `job_ingesta_reto01_dsh` con una tarea **Notebook** que apunte al generador y use cómputo compatible.
3. En los **parámetros del Job** agrega `sufijo = dsh` y `lote = lote_01`, sustituyendo `dsh` por tu sufijo. No dupliques estos parámetros en la tarea. Guarda el Job sin programación, con concurrencia máxima de una ejecución.
4. Cuando el instructor confirme la preparación, ejecuta el Job manualmente con **Run now** y espera a que termine correctamente. Genera tres eventos en un archivo nuevo dentro de tu carpeta. Si el volumen no existe o no tienes permiso, el Job falla con un mensaje que lo indica. El Job rechaza un sufijo o lote inválido y no sobrescribe un archivo existente: no ejecutes `lote_01` dos veces en la misma carpeta.

Los archivos de `datos/` son muestras de referencia equivalentes; no los subas si ya usas el Job, para no duplicar la ingesta.

## Paso a paso

1. **Genera el primer lote.** Ejecuta el Job manual descrito arriba con `lote = lote_01`. La salida debe decir `Lote lote_01: 3 eventos escritos en /Volumes/raw_data/bronze/landing_retos_sdp/dsh/reto01/entrada/lote_01.json`. Si dice que el volumen no existe, avisa al instructor.

2. **Crea el código.** En el workspace crea un archivo Python llamado `pipeline.py` con el contenido de `solucion/pipeline.py`. Si trabajas con Git folders, puedes importar este repositorio y usar directamente ese archivo. Identifica qué función crea cada dataset, en qué capa se publica y cómo se calculan las hash keys.

3. **Configura el pipeline.** Crea un pipeline Lakeflow llamado `pipeline_reto01_dsh`, selecciona el catálogo `raw_data` y el esquema `bronze` como destino por defecto, agrega el archivo Python como código fuente y selecciona ejecución **triggered**. Plata y Oro se publican en `data_vault.plata` y `ods.gold` porque el código usa el nombre completo de cada tabla. En **Settings → Configuration** agrega solo `pipelines.suffix = dsh`; el código rechaza un sufijo ausente o que no tenga exactamente tres letras. Las claves opcionales `raw_catalog`, `raw_schema`, `silver_catalog`, `silver_schema`, `gold_catalog`, `gold_schema` y `landing_volume` ya tienen los valores por defecto `raw_data`, `bronze`, `data_vault`, `plata`, `ods`, `gold` y `landing_retos_sdp`; cámbialas solo si el instructor lo indica.

4. **Ejecuta y observa.** Inicia una actualización normal, espera a que termine correctamente y revisa el grafo: Bronce alimenta el staging, el staging alimenta hub y satélite, y el satélite alimenta Oro. Si falla, consulta el mensaje de error antes de continuar.

5. **Valida con SQL.** Ejecuta en SQL Editor o un notebook, sustituyendo `dsh` por tu sufijo en cada nombre:

   ```sql
   SELECT COUNT(*) AS filas_bronce FROM raw_data.bronze.r01_vuelos_raw_dsh;
   SELECT COUNT(*) AS filas_hub    FROM data_vault.plata.r01_hub_vuelo_dsh;
   SELECT COUNT(*) AS filas_sat    FROM data_vault.plata.r01_sat_vuelo_dsh;
   SELECT * FROM ods.gold.r01_indicadores_vuelos_dsh;

   -- Columnas técnicas de Bronce
   SELECT evento_id, archivo_origen, fecha_archivo, fecha_ingesta
   FROM raw_data.bronze.r01_vuelos_raw_dsh ORDER BY evento_id;

   -- Hub y satélite se unen por la hash key
   SELECT h.evento_id, s.aeronave_id, s.velocidad_kmh, s.persona_alias, s.hashdiff
   FROM data_vault.plata.r01_hub_vuelo_dsh h
   JOIN data_vault.plata.r01_sat_vuelo_dsh s USING (hk_vuelo)
   ORDER BY h.evento_id;

   -- La hash key es el SHA-256 de la llave normalizada
   SELECT evento_id, hk_vuelo = sha2(upper(trim(evento_id)), 256) AS hash_correcto
   FROM data_vault.plata.r01_hub_vuelo_dsh;
   ```

   Resultados esperados: 3 filas en Bronce, 3 en el hub y 3 en el satélite; Oro con `eventos = 3`, `aeronaves = 2` y `velocidad_maxima = 300`; el join devuelve `e1`–`e3` y `hash_correcto` es `true` en las tres filas.

6. **Explica el diseño.** Indica por qué Bronce, hub y satélite son streaming tables, por qué el indicador es una vista materializada, por qué Data Vault separa la llave (hub) de los atributos (satélite) y cómo SDP determina el orden.

## Ingesta sintética cada 3 minutos

Después de completar las comprobaciones manuales:

1. Crea otro Job `job_programado_reto01_dsh` con una tarea **Notebook** llamada `generar_datos` que use `ingesta/generador.py`.
2. En los **parámetros del Job**, configura `sufijo = dsh`, `lote = continuo` y `registros = 500`. Sustituye el sufijo; no dupliques parámetros en la tarea. `registros` solo aplica a `continuo`, acepta enteros de **1 a 500** y vale 500 por defecto. Un valor inválido hace fallar la tarea antes de escribir.
3. Añade una tarea **Pipeline** llamada `transformar_datos`, dependiente del **éxito** de `generar_datos`, y selecciona `pipeline_reto01_dsh`. Usa actualización normal, **no full refresh**.
4. En **Add trigger → Scheduled → Advanced → Show Cron Syntax**, configura la expresión Quartz `0 0/3 * * * ?` y zona horaria `UTC`: minutos 00, 03, 06, etc. Guarda la programación inicialmente **pausada**.
5. Configura concurrencia máxima **1**, desactiva **Queue** y deja los reintentos automáticos de la tarea generadora en **0**. Si una ejecución dura más de 3 minutos, las siguientes pueden omitirse mientras sigue activa; no se superponen ni se acumulan. No se garantiza una ingesta exactamente cada 3 minutos.
6. Ejecuta **Run now** para verificar permisos y la cadena Notebook → Pipeline. Después activa la programación, observa algunas ejecuciones y vuelve a **pausarla** al terminar.

Cada ejecución exitosa del generador escribe **un archivo JSON Lines nuevo con 500 eventos** (o la cantidad elegida), sin sobrescribir. Los identificadores de evento, persona y archivo se generan con UUID e índice. Las aeronaves, países y velocidades varían usando solo valores dummy; no hay conexión a fuentes reales. Los lotes fijos `lote_01` no cambian.

Para comprobar la ingesta, revisa la ruta y el número de eventos en la salida del notebook y vuelve a ejecutar las consultas de validación. Con `registros = 500`, cada nueva ejecución exitosa seguida de su actualización normal agrega 500 filas a Bronce, al hub y al satélite; una actualización del pipeline **sin ejecutar el generador** no agrega eventos. Los indicadores se recalculan sobre los datos acumulados y ya no deben compararse con 3, 2 y 300.

El límite de 500 es **por ejecución del generador**, no para el total acumulado en Delta. Los reintentos manuales pueden producir otro lote; no se garantiza deduplicación de negocio. Esta programación aproxima near real-time, sin garantía de latencia: el planificador y el cómputo pueden retrasar ejecuciones y generan costos. Este repositorio no crea ni activa Jobs automáticamente; configura y valida la programación en tu workspace.

## Entregables y evaluación

- Captura o enlace al grafo y resultado de las consultas.
- Explicación breve de los tipos de dataset y del orden de ejecución.
- Aprobación: ejecución exitosa, grafo correcto, conteos 3/3/3 en Bronce, hub y satélite, y los tres indicadores esperados.
- Reflexión: ¿qué cambiarías para separar datos de pruebas de datos reales?

## Videos tutoriales en español

Ruta de estudio sugerida; los videos son material de apoyo, no sustituyen el paso a paso. Las duraciones son aproximadas y no están incluidas en la duración del reto. Enlaces y disponibilidad revisados el **25 de noviembre de 2026**; YouTube requiere conexión a Internet.

| Orden y video | Canal / duración | Qué estudiar y cómo aplicarlo |
|---|---|---|
| 1. [Introduction to Delta and Lakehouse — Workshop Práctico](https://www.youtube.com/watch?v=a9nZfTBatgc) | Databricks America Latina · 97 min · español, aunque el título está en inglés | Fundamentos de Delta Lake, tablas y procesamiento batch/streaming. Úsalo antes de crear Bronze y consultar sus indicadores. |
| 2. [Lakeflow: Declarative Pipelines & Connect — Workshop Práctico](https://www.youtube.com/watch?v=6ISOHWFoK8w) | Databricks America Latina · 104 min · español | Introducción práctica a pipelines declarativos y ETL. Concéntrate en la creación del pipeline y sus dependencias; Connect, CDC y los temas avanzados no son requisitos del reto. |
| 3. [Lakeflow Jobs en Databricks: Tasks, dependencias y parámetros](https://www.youtube.com/watch?v=CggUSoJ3fDQ) | EDUCATRONICOS ISC · 33 min · español | Crear un Job, ejecutar con Run now y pasar parámetros. Para la extensión, identifica la dependencia entre la tarea generadora y la tarea Pipeline. |

Para la extensión cada 3 minutos, consulta también [Lakeflow Jobs Parte 2: Triggers y Scheduled](https://www.youtube.com/watch?v=mXLtzno4tqo), de **EDUCATRONICOS ISC** (14 min, español). Estudia Scheduled; File Arrival y un notebook como orquestador son alternativas, no la configuración de este laboratorio.

**Compatibilidad y seguridad:** algunos videos usan el nombre anterior **Delta Live Tables (DLT)**, otras interfaces o ETL con notebooks. En este reto usa `from pyspark import pipelines as dp`, los decoradores de `solucion/pipeline.py`, Unity Catalog y `/Volumes/` como indica esta guía. El Job coordina tareas; SDP coordina datasets. No copies credenciales ni cargues datasets externos: conserva el generador dummy, tu sufijo y el límite de 500 registros por ejecución.

**Comprobación de estudio:** explica qué diferencia hay entre una streaming table y una vista materializada, y qué orden controla el Job frente al que determina SDP.

## Referencias

- [Tutorial oficial de pipelines](https://learn.microsoft.com/es-es/azure/databricks/getting-started/data-pipeline-get-started)
- [Funciones para definir datasets](https://learn.microsoft.com/es-es/azure/databricks/ldp/developer/definition-function)
- [Configuración de parámetros en Python](https://learn.microsoft.com/es-es/azure/databricks/ldp/parameters)
- [Tareas notebook en Jobs](https://learn.microsoft.com/es-es/azure/databricks/jobs/tasks/notebook)
- [Tareas pipeline en Jobs](https://learn.microsoft.com/es-es/azure/databricks/jobs/tasks/pipeline)
- [Programar Jobs](https://learn.microsoft.com/es-es/azure/databricks/jobs/scheduled)

La solución es una referencia para el facilitador o para consultar después del intento. Las vistas materializadas pueden actualizarse incrementalmente cuando son compatibles o recalcularse según el plan de ejecución.
