# radarlib.io.bufr.pyart_writer — Conversión de BUFR a Py-ART (Español)

Este módulo proporciona utilidades para convertir datos de radar decodificados en BUFR
(dicts de volumen) en objetos Py-ART `Radar` y opcionalmente guardarlos en archivos NetCDF CFRadial.

## Propósito

Después de decodificar uno o más archivos BUFR usando `radarlib.io.bufr.bufr`, obtienes una
lista de dicts de volumen con campos (reflectividad, velocidad, etc.) y metadatos. Este módulo
convierte tales dicts en un único objeto Py-ART `Radar` que se puede usar para análisis posterior,
visualización o guardar en un formato NetCDF estándar.

## Funciones principales

### `bufr_fields_to_pyart_radar(fields, *, include_scan_metadata=False, ...)`

Convierte una lista de dicts de campos decodificados en BUFR a un objeto Py-ART `Radar`.

**Entradas:**
- `fields` (List[dict]): Cada elemento debe contener `'data'` (ndarray de forma nrays × ngates) e `'info'` dict con metadatos BUFR.
- `include_scan_metadata` (bool): Cargar archivos de estrategia de escaneo XML si están disponibles (solo radares RMA).
- `root_scan_config_files` (Optional[Path]): Ruta al directorio que contiene archivos `.xml` de configuración.
- `config` (Optional[Dict]): Dict de configuración personalizado (ej. coordenadas del radar).
- `debug` (bool): Habilitar logging de debug.

**Salidas:**
- Un objeto `pyart.core.Radar` con todos los campos alineados a una grilla de rango común, elevaciones, acimutes, metadatos y coordenadas de tiempo por rayo.

**Comportamiento:**
- Selecciona automáticamente el "campo de referencia" — el que tiene la cobertura de rango más lejana.
- Alinea todos los otros campos a la grilla de referencia (rellena con NaN si tiene menos gates).
- Construye coordenadas de tiempo por rayo usando tiempos de inicio/final de barrido.
- Rellena parámetros del instrumento (PRT, ancho de pulso, ganancia de antena) para radares RMA si están disponibles los metadatos.

### `bufr_paths_to_pyart(bufr_paths, *, root_resources=None, ..., save_path=None)`

Wrapper de alto nivel: decodifica uno o más archivos BUFR y los convierte a objetos Py-ART.

**Entradas:**
- `bufr_paths` (List[str]): Lista de rutas de archivos BUFR.
- `root_resources` (Optional[str]): Ruta a recursos BUFR (libdecbufr.so, bufr_tables).
- `save_path` (Optional[Path]): Si se proporciona, guarda cada radar como archivo NetCDF CFRadial en este directorio.
- Otros argumentos: igual que `bufr_fields_to_pyart_radar`.

**Salidas:**
- Lista de tuplas `(bufr_path, radar_object)` para cada archivo decodificado exitosamente.

### `save_radar_to_cfradial(radar, out_file, format="NETCDF4")`

Guarda un objeto Py-ART `Radar` a un archivo NetCDF CFRadial.

**Entradas:**
- `radar`: Objeto Py-ART Radar.
- `out_file` (Path): Ruta del archivo de salida (debe terminar en `.nc`).
- `format` (str): Formato NetCDF ('NETCDF4', 'NETCDF3_CLASSIC', etc.).

**Salidas:**
- Devuelve el Path `out_file` en caso de éxito; lanza excepción en caso de fallo.

## Ejemplos de uso

### Convertir un archivo BUFR a Py-ART

```python
from radarlib.io.bufr.bufr import bufr_to_dict
from radarlib.io.bufr.pyart_writer import bufr_fields_to_pyart_radar

bufr_path = "tests/data/bufr/AR5_1000_1_DBZH_20240101T000746Z.BUFR"
vol = bufr_to_dict(bufr_path, root_resources="./bufr_resources")

radar = bufr_fields_to_pyart_radar([vol])
print(radar)  # <pyart.core.Radar object>
```

### Múltiples archivos BUFR con guardado a NetCDF

```python
from radarlib.io.bufr.pyart_writer import bufr_paths_to_pyart
from pathlib import Path

bufr_files = ["file1.BUFR", "file2.BUFR", "file3.BUFR"]
results = bufr_paths_to_pyart(
    bufr_files,
    root_resources="./bufr_resources",
    save_path=Path("./output_netcdf")
)

for bufr_path, radar in results:
    print(f"Convertido {bufr_path} → {radar.nrays} rayos, {radar.ngates} gates")
```

## Alineación de datos y selección de campo de referencia

Cuando múltiples productos (DBZH, VRAD, etc.) tienen números de gates u offsets diferentes:
- El módulo selecciona el producto con el rango más lejano (mayor `gate_offset + gate_size*ngates`).
- Todos los otros productos se interpolan/rellenan para coincidir con esta grilla de referencia usando arrays enmascarados.
- Se usa NaN para valores faltantes/rellenados.

Esto asegura una matriz de datos rectangular adecuada para herramientas estándar.

## Testing

- **Tests unitarios** (`tests/unit/test_pyart_writer.py`): Prueban la selección del campo de referencia y lógica de alineación con datos sintéticos.
- **Tests de integración** (`tests/integration/test_end_to_end_bufr_to_pyart.py`): Prueba end-to-end usando un archivo BUFR real y librería C (se salta si recursos no disponibles).

Ejecutar tests:
```bash
pytest tests/unit/test_pyart_writer.py
pytest tests/integration/test_end_to_end_bufr_to_pyart.py
```

## Dependencias

- `arm-pyart` — Toolkit de radar Py-ART (incluye módulo `pyart`).
- `netCDF4` — E/S de archivos NetCDF (requerido por Py-ART para escritura de CFRadial).
- `numpy`, `pandas` — Operaciones con arrays y dataframes.

## Notas

- La implementación actual usa `pyart.testing.make_empty_ppi_radar()` para crear el objeto Radar, que asume un tipo de escaneo PPI (plan position indicator). Para RHI u otros tipos de escaneo, puede ser necesaria más personalización.
- Los metadatos de archivos de configuración XML son opcionales y solo se cargan cuando se solicita explícitamente para radares RMA.
- Los metadatos no serializables (objetos dict) se eliminan antes de escribir a NetCDF para evitar errores de tipo en la librería netCDF4.
