import datetime
from datetime import timezone
import pytz
from radarlib import config
import os

tz_utc = pytz.timezone('UTC')
tz_arg = pytz.timezone('America/Argentina/Cordoba')

def get_time_from_RMA_filename(filename, tz_UTC=True):
    """
    Extract datetime from RMA BUFR filename.
    """
    str_time = filename.split('_')[3].split('.')[0]
    date = datetime.datetime.strptime(str_time, "%Y%m%dT%H%M%SZ")

    # el huso horario de los vols rma es UTC
    date = date.replace(tzinfo=timezone.utc)

    if not tz_UTC:
        # trasladamos tiempo a huso horario argentino
        date = date.astimezone(tz_arg)

    return date

def get_path_from_RMA_filename(filename, **kwargs):
    root_radar_files = kwargs.get('root_radar_files')
    if root_radar_files is None:
        root_radar_files = config.ROOT_RADAR_FILES_PATH

    radar = filename.split('_')[0]
    ano = filename.split('_')[3].split('T')[0][0:4]
    mes = filename.split('_')[3].split('T')[0][4:6]
    dia = filename.split('_')[3].split('T')[0][6:8]
    hora = filename.split('_')[3].split('T')[1][0:2]

    path = os.path.join(root_radar_files, radar, ano, mes, dia, hora)
    return path