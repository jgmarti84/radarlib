import logging
from typing import Any, Dict, List, Optional, Tuple
import numpy as np

def dec_bufr_file(
        bufr_filename: str,
        root_resources: str = "./bufr_resources/",
        logger_name: Optional[str] = None,
        parallel: bool = True
    ) -> Tuple[Dict[str, Any], List[dict], np.ndarray, List[List[Any]]]:
    """
    Decodifica un archivo BUFR usando la librería C y reconstruye:
    - metadatos del volumen (meta_vol),
    - una lista de barridos (meta_sweeps) con sus datos descomprimidos,
    - el volumen completo concatenado (vol_data) y
    - un registro de ejecución (run_log) con advertencias/errores.

    Args:
        bufr_filename: Ruta al archivo BUFR a procesar.
        root_resources: Ruta al directorio de recursos (tablas y librería C).
        logger_name: Nombre base para el logger.
        parallel: Si True, usa ThreadPoolExecutor para descomprimir sweeps
            en paralelo (por barrido). Si False, procesa en serie.

    Returns:
        Tupla (meta_vol, sweeps, vol_data, run_log):
            - meta_vol: diccionario con metadatos del volumen
            - sweeps: lista de diccionarios por barrido (con 'data')
            - vol_data: ndarray 2-D con los datos concatenados
            - run_log: lista con entradas de log (niveles/mensajes)

    Nota:
        No se modifica la lógica de validación existente; las excepciones
        se registran en el logger y se propagan cuando corresponda.
    """
    filename = bufr_filename.split('/')[-1]
    logger = logging.getLogger((logger_name or __name__) + '.' + filename.split('_')[0])

    run_log = []

    try:
        with decbufr_library_context(root_resources) as lib:
            # Extrae metadatos generales del volumen
            vol_metadata = get_metadata(lib, bufr_filename, root_resources)

            # Extrae tamaño del volumen, datos crudos y elevaciones
            size_data = get_size_data(lib, bufr_filename, root_resources)
            vol = get_raw_volume(lib, bufr_filename, root_resources, size=size_data)
            elevs = get_elevations(lib, bufr_filename, root_resources, max_elev=vol[0])

            # Encabezado de barridos + datos comprimidos
            nsweeps = int(vol[0])
            vol_metadata["nsweeps"] = nsweeps
            sweeps = parse_sweeps(vol, nsweeps, elevs)

            def decompress_wrapper(sw, idx):
                try:
                    sw["data"] = decompress_sweep(sw)
                    return sw, None
                except SweepConsistencyException as e:
                    vol_name = bufr_filename.split('/')[-1].split('.')[0][:-5]
                    product_type = sw.get("product_type", "N/A")
                    message = f"{vol_name}: Se descarta barrido inconsistente ({product_type} / Sw: {idx}) (ngates fuera de limites)"
                    logger.warning(message)
                    return None, [2, message]
                except Exception as e:
                    logger.warning(f"Descartado barrido inconsistente en sweep {idx}: {e}")
                    return None, [2, f"Descartado barrido inconsistente en sweep {idx}: {e}"]

            results = []
            if parallel:
                from concurrent.futures import ThreadPoolExecutor, as_completed
                with ThreadPoolExecutor() as executor:
                    futures = {executor.submit(decompress_wrapper, sw, idx): idx for idx, sw in enumerate(sweeps)}
                    for future in as_completed(futures):
                        sw, log_entry = future.result()
                        if sw is not None:
                            results.append(sw)
                        if log_entry:
                            run_log.append(log_entry)
                # To preserve sweep order, sort by original index if needed
                results.sort(key=lambda sw: sweeps.index(sw))
            else:
                for idx, sw in enumerate(sweeps):
                    sw_out, log_entry = decompress_wrapper(sw, idx)
                    if sw_out is not None:
                        results.append(sw_out)
                    if log_entry:
                        run_log.append(log_entry)
            sweeps = results

            # Actualiza número de barridos y metadatos desde nombre de archivo
            vol_metadata["nsweeps"] = len(sweeps)
            name_dict = bufr_name_metadata(bufr_filename)
            vol_metadata = dict(vol_metadata, **name_dict)

            # Uniformiza el número de ngates entre barridos (rellena con NaN si es menor)
            sweeps = uniformize_sweeps(sweeps)

            # Arma el volumen concatenando todos los barridos
            vol_data = assemble_volume(sweeps)

            return vol_metadata, sweeps, vol_data, run_log

    except Exception as e:
        logger.error(f"Error en la decodificacion del archivo BUFR: {e}", exc_info=True)
        run_log.append([3, str(e)])
        raise ValueError(f"Error en la decodificacion del archivo BUFR: {e}")
    
def bufr_to_dict(bufr_filename: str, root_resources: str = "./bufr_resources", logger_name: str = None, legacy=False) -> Optional[dict]:
    """
    Procesa un archivo BUFR y devuelve una representación en forma de diccionario
    lista para uso por otras partes del pipeline.

    El diccionario resultante contiene al menos:
      - 'data': ndarray 2-D con todos los datos concatenados.
      - 'info': diccionario con metadatos y listas por barrido.

    Args:
        bufr_filename: Ruta al archivo BUFR a procesar.
        root_resources: Ruta al directorio de recursos (por defecto './bufr_resources').
        logger_name: Nombre del logger a utilizar (si se desea).

    Returns:
        Diccionario con 'data' e 'info' o None en caso de fallo
        (el error queda registrado en el logger).
    """
    # TODO: include a check for input type. bufr_filename should be str or Path and not a list nor dict. Potentially include a fallback in case it's a list of strings
    filename = bufr_filename.split('/')[-1]
    logger_local = logging.getLogger((logger_name or __name__) + '.' + filename.split('_')[0])
    # Implement retry/backoff for transient failures (e.g., I/O, C-library transient errors)
    max_attempts = 3
    base_delay = 0.5
    for attempt in range(1, max_attempts + 1):
        try:
            meta_vol, meta_sweeps, vol_data, run_log = dec_bufr_file(bufr_filename=bufr_filename, root_resources=root_resources, logger_name=logger_name)

            vol: Dict[str, Any] = {"data": vol_data}

            vol["info"] = build_info_dict(meta_vol, meta_sweeps)
            if legacy:
                vol["info"] = dict(vol["info"], **vol["info"]["sweeps"].to_dict(orient="list"))
                del vol["info"]["sweeps"]

            return vol

        except Exception as e:
            # Log with local logger including attempt count
            logger_local.warning(f"Attempt {attempt}/{max_attempts} failed for {bufr_filename}: {e}")
            if attempt < max_attempts:
                # exponential backoff with jitter
                delay = base_delay * (2 ** (attempt - 1))
                delay = delay * (0.8 + 0.4 * np.random.random())
                time.sleep(delay)
                continue
            else:
                logger_local.error("Error en bufr_to_dict (final): %s", e, exc_info=True)
                # attach to run_log for compatibility (if exists)
                try:
                    run_log.append([3, str(e)])
                except Exception:
                    pass
                return None