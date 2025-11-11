# radarlib.io.bufr.bufr — Utilidades de decodificación BUFR (Español)

Este módulo proporciona funciones para decodificar archivos BUFR de radar
usando una librería C compartida (`libdecbufr.so`) junto a utilidades para
parsear, descomprimir y ensamblar los datos de barridos (sweeps) en arrays de
numpy y un diccionario `info` estructurado para su uso en el pipeline.

Contenidos
- Propósito y flujo general
- Tipos y excepciones importantes
- Funciones públicas (firma, descripción, valores devueltos)
- Ejemplos de uso
- Errores, casos límite y notas de testeo
- Despliegue (librería C y recursos)

Propósito y flujo general
------------------------
`radarlib.io.bufr.bufr` actúa como una capa 'pegamento' en Python alrededor de
un decodificador C para archivos BUFR. El flujo típico es:

1. Cargar `libdecbufr.so` (librería C incluida en los recursos del proyecto).
2. Llamar funciones C para leer tamaño del volumen, buffer de enteros y
   ángulos fijos (elevations).
3. Parsear el buffer de enteros en encabezados por barrido y trozos comprimidos.
4. Descomprimir los bytes por barrido con `zlib` y convertirlos en arrays 2-D
   (rays × gates).
5. Uniformizar el número de gates por barrido, concatenar verticalmente los
   barridos en un volumen 2-D y construir el diccionario `info`.

Tipos y excepciones importantes
------------------------------
- `SweepConsistencyException` — lanzada cuando un barrido tiene un número de
  gates inválido (se usa para descartar barridos malos).
- `point_t`, `meta_t` — definiciones `ctypes.Structure` usadas por la API C.

Funciones públicas
------------------
Listado de las funciones principales y sus contratos (resumen):

- `decbufr_library_context(root_resources: Optional[str] = None) -> CDLL context`
  - Context manager que devuelve un `ctypes.CDLL` ya cargado.

- `bufr_name_metadata(bufr_filename: str) -> dict`
  - Parsea el nombre de archivo con patrón `RADAR_STRATEGY_NVOL_TYPE_TIMESTAMP.BUFR`.

- `load_decbufr_library(root_resources: str) -> CDLL`
  - Carga `dynamic_library/libdecbufr.so` desde el directorio de recursos.

- `get_metadata(lib: CDLL, bufr_path: str, root_resources: Optional[str]) -> dict`
  - Llama a `get_meta_data` en la librería C y devuelve año/mes/día/hora/min,
    latitud/longitud y altura del radar.

- `get_elevations(... ) -> np.ndarray` — devuelve elevaciones (fixed angles).
- `get_raw_volume(... ) -> np.ndarray` — devuelve el buffer entero crudo.
- `get_size_data(... ) -> int` — tamaño esperado del buffer crudo.
- `parse_sweeps(vol, nsweeps, elevs) -> list[dict]` — parsea el buffer entero
  en cabeceras por sweep y un `compress_data` con los bytes comprimidos.
- `decompress_sweep(sweep) -> np.ndarray` — descomprime y transforma a 2-D
  `(nrays, ngates)`. Lanza `SweepConsistencyException` o `ValueError` según el
  problema.
- `uniformize_sweeps(sweeps)` — rellena con `NaN` para que todos compartan
  el mismo `ngates`.
- `assemble_volume(sweeps)` — concatena verticalmente los barridos en un
  volumen numpy 2-D.
- `validate_sweeps_df(sweeps_df)` — validaciones básicas entre sweeps.
- `build_metadata(filename, info)` — construye metadatos estandarizados.
- `build_info_dict(meta_vol, meta_sweeps)` — arma el `info` final usado por
  consumidores.
- `dec_bufr_file(bufr_filename, root_resources=None, logger_name=None, parallel=True)`
  - Función principal que ejecuta el flujo completo, opcionalmente descomprimiendo
    sweeps en paralelo. Devuelve `(meta_vol, sweeps, vol_data, run_log)`.
- `bufr_to_dict(bufr_filename, root_resources=None, logger_name=None, legacy=False)`
  - Wrapper con reintentos y backoff; devuelve `{'data', 'info'}` o `None`.

Ejemplos de uso
---------------
Uso básico:

```python
from radarlib.io.bufr.bufr import bufr_to_dict

result = bufr_to_dict('tests/data/bufr/AR5_1000_1_DBZH_20240101T000746Z.BUFR')
if result is None:
    print('falló el procesamiento')
else:
    data = result['data']
    info = result['info']
```

Errores y casos límite
----------------------
- `cdll.LoadLibrary` lanzará `OSError` si `libdecbufr.so` no existe o es inválida.
- `decompress_sweep` lanzará `ValueError` si la longitud de datos descomprimidos
  no coincide con `nrays * ngates`.
- `SweepConsistencyException` se usa para filtrar barridos con `ngates`
  implausibles.

Notas de testing
----------------
- Los tests unitarios en el repo usan monkeypatch para simular la librería C.
- Las pruebas de integración requieren un archivo `.BUFR` real y la biblioteca
  `libdecbufr.so` disponible en `tests/data` o en la ruta configurada.

Despliegue y empaquetado
------------------------
- Asegurarse de incluir `dynamic_library/libdecbufr.so` y `bufr_tables` en
  `package_data`/wheel para que estén disponibles tras la instalación.
- Para pre-commit en CI, instalar `isort` o usar un hook `local`/`system` para
  evitar errores de checkout de mirrors.

Mejoras recomendadas
--------------------
- Añadir validación de tipos de entrada (`Path` vs `str`).
- Tipar retornos con `TypedDict`.
- Mejorar manejo de errores y mensajes de `run_log` para facilitar debug.
