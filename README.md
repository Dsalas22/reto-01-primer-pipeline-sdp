# Reto 1: mi primer pipeline declarativo

**Nivel:** inicial · **Duración sugerida:** 1–2 horas  
**Modalidad:** individual o en parejas.

## Objetivo

Crear un pipeline de Lakeflow Spark Declarative Pipelines que ingiera eventos de vuelos sintéticos en una streaming table y publique indicadores en una vista materializada. El propósito es comprender las dependencias, no conectarse a una fuente real.

## Resultado esperado

Con el archivo incluido: 3 eventos, 2 aeronaves distintas y velocidad máxima de 300. El grafo debe mostrar `vuelos_bronze → indicadores_vuelos`.

## Requisitos

- Workspace de Databricks con Unity Catalog, Lakeflow pipelines y Auto Loader habilitados, y cómputo compatible.
- Un catálogo y esquema propios o asignados. Permisos para usarlos, crear una streaming table, una vista materializada y un volumen, y leer/escribir en el volumen.
- Acceso a SQL Editor o a un notebook y permiso para ejecutar el pipeline. El uso de cómputo y almacenamiento puede generar costos.
- Conocimientos básicos de Python y SQL. Los datos son sintéticos y no requieren una API externa.

Usa un esquema y una carpeta de entrada independientes por participante y por reto: los tres pipelines reutilizan algunos nombres de tabla. En todos los ejemplos sustituye `CATALOGO` y `ESQUEMA` por tus valores reales, sin los símbolos `< >`.

## Paso a paso

1. **Prepara el volumen.** En SQL Editor o un notebook SQL ejecuta:

   ```sql
   CREATE VOLUME IF NOT EXISTS CATALOGO.ESQUEMA.retos_sdp;
   ```

   En Catalog Explorer crea la carpeta `reto01/entrada` dentro del volumen y sube allí `datos/lote_01.json`. La ruta completa será `/Volumes/CATALOGO/ESQUEMA/retos_sdp/reto01/entrada`. La carpeta debe comenzar vacía.

2. **Crea el código.** En el workspace crea un archivo Python llamado `pipeline.py` con el contenido de `solucion/pipeline.py`. Si trabajas con Git folders, puedes importar este repositorio y usar directamente ese archivo. Identifica qué función crea cada dataset y cómo se leen sus datos.

3. **Configura el pipeline.** Crea un pipeline Lakeflow, selecciona el catálogo y esquema destino, agrega el archivo Python como código fuente y selecciona ejecución **triggered**. En **Settings → Configuration**, agrega la clave `lab.source_path` con la ruta de la carpeta de entrada del paso 1. Usa la sección Configuration, no parámetros SQL.

4. **Ejecuta y observa.** Inicia una actualización normal, espera a que termine correctamente y revisa el grafo. Si falla, consulta el mensaje de error antes de continuar.

5. **Valida con SQL.** Ejecuta en SQL Editor o un notebook, sustituyendo el catálogo y esquema:

   ```sql
   SELECT * FROM CATALOGO.ESQUEMA.indicadores_vuelos;
   SELECT COUNT(*) AS eventos, COUNT(DISTINCT aeronave_id) AS aeronaves,
          MAX(velocidad_kmh) AS velocidad_maxima
   FROM CATALOGO.ESQUEMA.vuelos_bronze;
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
