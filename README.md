# Reto 1: mi primer pipeline declarativo

**Nivel:** inicial · **Duración sugerida:** 1–2 horas  
**Modalidad:** individual o en parejas.

## Objetivo

Crear un pipeline de Lakeflow Spark Declarative Pipelines que ingiera eventos de vuelos sintéticos en una streaming table y publique indicadores en una vista materializada. El propósito es comprender las dependencias, no conectarse a una fuente real.

## Resultado esperado

Con el archivo incluido: 3 eventos, 2 aeronaves distintas y velocidad máxima de 300. Para el sufijo de ejemplo `dsh`, el grafo muestra `vuelos_bronze_dsh → indicadores_vuelos_dsh`.

## Requisitos

- Workspace de Databricks con Unity Catalog, Lakeflow pipelines y Auto Loader habilitados, y cómputo compatible.
- Un catálogo y esquema propios o asignados. Permisos para usarlos, crear una streaming table, una vista materializada y un volumen, y leer/escribir en el volumen. Crear el catálogo requiere permisos adicionales; puede prepararlo el administrador.
- Acceso a SQL Editor o a un notebook y permiso para ejecutar el pipeline. El uso de cómputo y almacenamiento puede generar costos.
- Conocimientos básicos de Python y SQL. Los datos son sintéticos y no requieren una API externa.

## Identificación por participante

Forma un sufijo con la primera letra del nombre y de cada uno de los dos apellidos, en minúsculas y sin tildes; por ejemplo, `dsh`. Usa tres letras ASCII (`a`–`z`). Si dos participantes tienen las mismas iniciales o una persona no puede formar tres letras, el instructor asignará a cada uno un sufijo alternativo de tres letras **único**. Confírmalo antes de crear recursos.

Todos los ejemplos usan `dsh`: sustitúyelo por tu sufijo en **cada** nombre, consulta y ruta. El catálogo `retos_sdp_dsh` es personal y puede usarse en los tres retos; el esquema `reto01_dsh`, el volumen, la carpeta y el pipeline son específicos de este reto. La separación por nombres no reemplaza permisos de Unity Catalog ni protege frente al acceso de otros usuarios.

Los pipelines existentes con nombres anteriores no se migran automáticamente: crea un pipeline nuevo con sus recursos propios; no cambies el sufijo en un pipeline compartido o ya ejecutado.

## Paso a paso

1. **Prepara catálogo, esquema y volumen.** El administrador crea o asigna el catálogo personal `retos_sdp_dsh`; verifica con él que no esté asignado a otra persona. Si tienes los permisos necesarios, ejecuta en SQL Editor o un notebook SQL:

   ```sql
   CREATE CATALOG IF NOT EXISTS retos_sdp_dsh;
   CREATE SCHEMA IF NOT EXISTS retos_sdp_dsh.reto01_dsh;
   CREATE VOLUME IF NOT EXISTS retos_sdp_dsh.reto01_dsh.volumen_sdp_dsh;
   ```

   Si el catálogo ya fue provisionado, omite la primera sentencia. En Catalog Explorer crea la carpeta `reto01_dsh/entrada_dsh` dentro del volumen y sube allí `datos/lote_01.json`. La ruta completa será `/Volumes/retos_sdp_dsh/reto01_dsh/volumen_sdp_dsh/reto01_dsh/entrada_dsh`. La carpeta debe comenzar vacía.

2. **Crea el código.** En el workspace crea un archivo Python llamado `pipeline.py` con el contenido de `solucion/pipeline.py`. Si trabajas con Git folders, puedes importar este repositorio y usar directamente ese archivo. Identifica qué función crea cada dataset y cómo se leen sus datos.

3. **Configura el pipeline.** Crea un pipeline Lakeflow llamado `pipeline_reto01_dsh`, selecciona el catálogo `retos_sdp_dsh` y esquema `reto01_dsh` como destino, agrega el archivo Python como código fuente y selecciona ejecución **triggered**. En **Settings → Configuration**, agrega `pipelines.suffix = dsh` y `lab.source_path` con la ruta del paso 1. El código rechaza un sufijo ausente o que no tenga exactamente tres letras. Usa Configuration, no parámetros SQL.

4. **Ejecuta y observa.** Inicia una actualización normal, espera a que termine correctamente y revisa el grafo. Si falla, consulta el mensaje de error antes de continuar.

5. **Valida con SQL.** Ejecuta en SQL Editor o un notebook, sustituyendo `dsh` por tu sufijo en cada nombre:

   ```sql
   SELECT * FROM retos_sdp_dsh.reto01_dsh.indicadores_vuelos_dsh;
   SELECT COUNT(*) AS eventos, COUNT(DISTINCT aeronave_id) AS aeronaves,
          MAX(velocidad_kmh) AS velocidad_maxima
   FROM retos_sdp_dsh.reto01_dsh.vuelos_bronze_dsh;
   ```

   Ambas consultas deben producir 3, 2 y 300 para los indicadores equivalentes.

6. **Explica el diseño.** Indica por qué la ingesta es una streaming table, por qué el indicador es una vista materializada y cómo SDP determina el orden.

## Entregables y evaluación

- Captura o enlace al grafo y resultado de las consultas.
- Explicación breve de los tipos de dataset y del orden de ejecución.
- Aprobación: ejecución exitosa, grafo correcto y los tres indicadores esperados.
- Reflexión: ¿qué cambiarías para separar datos de pruebas de datos reales?

## Referencias

- [Tutorial oficial de pipelines](https://docs.databricks.com/aws/en/getting-started/data-pipeline-get-started)
- [Funciones para definir datasets](https://docs.databricks.com/aws/en/ldp/developer/definition-function)
- [Configuración de parámetros en Python](https://docs.databricks.com/aws/en/ldp/parameters)

La solución es una referencia para el facilitador o para consultar después del intento. Las vistas materializadas pueden actualizarse incrementalmente cuando son compatibles o recalcularse según el plan de ejecución.
