'''
ALMA monitoring database (text-based) to temporary AMG time series database (InfluxDB) data collector.
Data on AMG time series database is used for fast predefined data analysis, visualizations (Grafana dashboards) and
fault detection.

This process is meant to be run on a cron job collecting a small selection of monitoring points of interest.
Monitoring points to collect are defined in accompanying configuration file.

Data collection from ALMA monitoring database (loosely) based on AMG's MonitorPlotter application, later adapted
for Dataiku's LO2DRIFTAUTO project.
'''

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
import lib.BaseLogger as BaseLogger
from config.MonitorPoints import monitorPoints
import config.definitions as cfg

DEBUG = cfg.DEBUG

def main():
    logger = BaseLogger.createLogger('mon-to-influx', rotator=True)
    if DEBUG:
        logger.addHandler(BaseLogger.getStdoutHandler())

    logger.info(f'Starting mon-to-influx data collection. {len(monitorPoints)} monitor points defined.')

    # Data collector from ALMA monitoring database (based on MonitorPlotter application)
    fetcher = DataFetch()

    for measurement in monitorPoints:
        plotData = {'abm': measurement['abm'], 'lru': measurement['lru'], 'monitor': measurement['mon']}
        data = fetcher.getData(plotData, daysBack=7) #, strDateEnd='2019-08-01 00:00:00')

        # Remove microseconds part of timestamp to reduce storage
        # Alternatively use: lambda t: t.strftime('%Y-%m-%d %H:%M:%S')
        # data.index = data.index.map(lambda x: x.replace(microsecond=0))

        dataset = []
        for row in data.itertuples():
            dataset.append(cfg.MEASUREMENT_BASE.format(measurement=measurement['mon_alias'],
                                                       abm=measurement['abm_alias'],
                                                       lru=measurement['lru_alias'],
                                                       value=row.col1,
                                                       timestamp=int(row.Index.timestamp())))
                                                                        # ^--- for ns precision, use: int(row.Index.to_datetime64()
        # Push measurements to database
        if dataset:
            measurements = '\n'.join(dataset)
            # Store measurements in timeseries database
            if not DEBUG:
                try:
                    response = requests.post(cfg.TIMESERIES_DB_API_URL, data=measurements)
                    if response.status_code == cfg.REQUEST_STATUS_CODE_OK:
                        logger.info(f"Measurement {measurement['mon_alias']},{measurement['abm_alias']},{measurement['lru_alias']} correctly " + \
                                    f"stored in database: {len(dataset)} records, timestamp {datetime.now().isoformat()}")
                        pass
                    else:
                        logger.error('Error storing measurements in database')
                except:
                    logger.exception('Error storing measurements in timeseries database')
            else:
                logger.debug('Simulation mode:')
                # logger.debug(measurements)


if __name__ == '__main__':
    sys.exit(main())
