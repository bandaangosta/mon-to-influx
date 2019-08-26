# TODO: add docstring
# TODO: add logging

import os
import sys

# Adds application root to path
PATH_ROOT = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
if not PATH_ROOT in sys.path:
    sys.path.insert(1, PATH_ROOT)

# Activate Python virtual environment containing all needed libraries and dependencies
if os.path.exists(os.path.join(PATH_ROOT, 'venv/bin/activate_this.py')):
    activate_this = os.path.join(PATH_ROOT, 'venv', 'bin', 'activate_this.py')
else:
    print('Virtual environment not found')
    raise SystemExit
exec(open(activate_this).read(), {'__file__': activate_this})

from datetime import datetime
import requests
import traceback
from lib.DataFetch import DataFetch

# TODO: move this to separate config file
# Timeseries DB definitions
TIMESERIES_DB_HOST = '172.17.0.1'
TIMESERIES_DB_PORT = '8086'
TIMESERIES_DB_NAME = 'cloa_monitor_ict'
TIMESERIES_DB_API_URL = 'http://{}:{}/write?db={}&precision=s'.format(TIMESERIES_DB_HOST, TIMESERIES_DB_PORT, TIMESERIES_DB_NAME)
MEASUREMENT_BASE = '{measurement},abm={abm},lru={lru} value={value:.3f} {timestamp}'
REQUEST_STATUS_CODE_OK = 204

fetcher = DataFetch()
# data.deletePreviousData()

# Antennas list for iteration
CMs = ['CM%02d' % (x + 1) for x in range(12)]
DAs = ['DA%02d' % (x + 41) for x in range(25)]
DVs = ['DV%02d' % (x + 1) for x in range(25)]
PMs = ['PM%02d' % (x + 1) for x in range(4)]
allABMs = CMs + DAs + DVs + PMs

# TODO: move this to separate config file
monitorPoints = [
    {
        'abm'       : 'CentralLO',
        'abm_alias' : 'CLO',
        'lru'       : 'ML_930008016217fc10',
        'lru_alias' : 'ML1',
        'mon'       : 'SIGNAL_PM_DC_10V',
        'mon_alias' : 'SIGNAL_PM_DC_10V'
    },
    {
        'abm'       : 'CentralLO',
        'abm_alias' : 'CLO',
        'lru'       : 'ML_930008016217fc10',
        'lru_alias' : 'ML1',
        'mon'       : 'SIGNAL_PPLN_TEMP_MON',
        'mon_alias' : 'SIGNAL_PPLN_TEMP_MON'
    },
    {
        'abm'       : 'CentralLO',
        'abm_alias' : 'CLO',
        'lru'       : 'ML_930008016217fc10',
        'lru_alias' : 'ML1',
        'mon'       : 'SIGNAL_RED_PD_PWR_MON',
        'mon_alias' : 'SIGNAL_RED_PD_PWR_MON'
    },
    {
        'abm'       : 'CentralLO',
        'abm_alias' : 'CLO',
        'lru'       : 'ML2_3000080205eba710',
        'lru_alias' : 'ML2',
        'mon'       : 'SIGNAL_PM_DC_10V',
        'mon_alias' : 'SIGNAL_PM_DC_10V'
    },
    {
        'abm'       : 'CentralLO',
        'abm_alias' : 'CLO',
        'lru'       : 'ML2_3000080205eba710',
        'lru_alias' : 'ML2',
        'mon'       : 'SIGNAL_PPLN_TEMP_MON',
        'mon_alias' : 'SIGNAL_PPLN_TEMP_MON'
    },
    {
        'abm'       : 'CentralLO',
        'abm_alias' : 'CLO',
        'lru'       : 'ML2_3000080205eba710',
        'lru_alias' : 'ML2',
        'mon'       : 'SIGNAL_RED_PD_PWR_MON',
        'mon_alias' : 'SIGNAL_RED_PD_PWR_MON'
    },
]

for measurement in monitorPoints:
    plotData = {'abm': measurement['abm'], 'lru': measurement['lru'], 'monitor': measurement['mon']}
    data = fetcher.getData(plotData, daysBack=0)

    # Remove microseconds part of timestamp to reduce storage
    # Alternatively use: lambda t: t.strftime('%Y-%m-%d %H:%M:%S')
    # data.index = data.index.map(lambda x: x.replace(microsecond=0))

    dataset = []
    for row in data.itertuples():
        # dataset.append(MEASUREMENT_BASE.format(measurement=measurement['mon_alias'], abm=measurement['abm_alias'], lru=measurement['lru_alias'], value=row.col1, timestamp=int(row.Index.to_datetime64())))
        dataset.append(MEASUREMENT_BASE.format(measurement=measurement['mon_alias'],
                                               abm=measurement['abm_alias'],
                                               lru=measurement['lru_alias'],
                                               value=row.col1,
                                               timestamp=int(row.Index.timestamp())))
                                                                # ^--- for ns precision, use: int(row.Index.to_datetime64()
    # Push measurements to database
    if dataset:
        measurements = '\n'.join(dataset)
        # Store measurements in timeseries database
        # if not cfg.SIMULATED:
        if True:
            try:
                response = requests.post(TIMESERIES_DB_API_URL, data=measurements)
                if response.status_code == REQUEST_STATUS_CODE_OK:
                    # print('Measurements correctly stored in database: {} records, timestamp {}'.format(len(dataset), datetime.now().isoformat()))
                    print(f"Measurement {measurement['mon_alias']},{measurement['abm_alias']},{measurement['lru_alias']} correctly " + \
                          f"stored in database: {len(dataset)} records, timestamp {datetime.now().isoformat()}")
                    pass
                else:
                    print('Error storing measurements in database')
            except:
                print(traceback.print_exc())
                print('Error storing measurements in timeseries database')
        else:
            print('Simulation mode:')
            print(measurements)
