#!/usr/bin/env python3

'''
ALMA monitoring database (text-based) to temporary AMG time series database (InfluxDB) data collector.
Data on AMG time series database is used for fast predefined data analysis, visualizations (Grafana dashboards) and
fault detection.

This process is meant to be run on a cron job collecting a small selection of monitoring points of interest.
Monitoring points to collect are defined in accompanying configuration file.

Data collection from ALMA monitoring database (loosely) based on AMG's MonitorPlotter application, later adapted
for Dataiku's LO2DRIFTAUTO project.
'''

# import pdb; pdb.set_trace()

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
import math
import re
import requests
import traceback
import typer
from lib.DataFetch import DataFetch
import lib.BaseLogger as BaseLogger
import lib.CacheHandler as CacheHandler
import lib.DateTools as DateTools
from config.MonitorPoints import monitorPoints
import config.definitions as cfg

DEBUG = cfg.DEBUG

def chunks(lst, n):
    '''Yield successive n-sized chunks from lst.'''
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def main(
    daysBack: int = typer.Option(help='Number of days back from now to retrieve', default=1, show_default=True),
    dateEnd: str = typer.Option(help='Instead of now, retrieve data back from this moment. Format is yyyy-mm-dd hh:mm:ss', default=DateTools.dateToStr(datetime.now()))
):
    '''
    ALMA monitoring database (text-based) to temporary AMG time series database (InfluxDB) data collector.
    Data on AMG time series database is used for fast predefined data analysis, visualizations (Grafana dashboards) and
    fault detection.

    This process is meant to be run on a cron job collecting a small selection of monitoring points of interest.
    Monitoring points to collect are defined in accompanying configuration file MonitorPoints.py.

    Data collection from ALMA monitoring database (loosely) based on AMG's MonitorPlotter application, later adapted
    for Dataiku's LO2DRIFTAUTO project.
    '''

    logger = BaseLogger.createLogger('mon-to-influx', rotator=True)
    if DEBUG:
        logger.addHandler(BaseLogger.getStdoutHandler())

    logger.info(f'Started mon-to-influx data collection. {len(monitorPoints)} monitor points defined.')

    # Data collector from ALMA monitoring database (based on MonitorPlotter application)
    fetcher = DataFetch()

    # Cache database connection to manage monitor point querying expiration time
    cache = CacheHandler.CacheHandler(cfg.CACHE_HOST)

    logger.info(f'Obtaining data for {daysBack} days back from {dateEnd}')

    # Iterate over all monitor points to query as defined in MonitorPoints.py
    for measurement in monitorPoints:
        # Skip this measurement if not enough time has passed since last data retrieval
        ttl = cache.getTtlHrs('{}:{}:{}'.format(measurement['abm'], measurement['lru'], measurement['mon']))
        if ttl:
            logger.info(f"Measurement {measurement['abm']}:{measurement['lru']}:{measurement['mon']} must wait {ttl:.2f} hours until obtained again")
            continue

        # ABM to retrieve data from can be a single value (e.g., DV01 or WeatherStationController) or <all_ants> placeholder
        # which must be expanded to the full antenna list
        if measurement['abm'] == '<all_ants>':
            abms = cfg.allABMs
            abmsAlias = cfg.allABMs
        else:
            abms = [measurement['abm']]
            abmsAlias = [measurement['abm_alias']]

        # LRU to retrieve data from can be a single value (e.g., SIGNAL_RED_PD_PWR_MON) or a pseudo-regular expression
        # in format 'some_text<i-j>' which expands to 'some_text(i)', 'some_text(i+1)', ..., 'some_text(j)' with i
        # and j integers greater than zero. This allows to iterate over polarizations, baseband pairs, etc.
        # Example: 'LO2BBpr<0-3>' generates the list ['LO2BBpr0', 'LO2BBpr1', 'LO2BBpr2', 'LO2BBpr3']
        pattern = re.compile(r"(.*)<([0-9]*)-([0-9]*)>")
        results = pattern.findall(measurement['lru'])
        lrus = []
        if len(results) > 0:
            for i in range(int(results[0][1]), int(results[0][2]) + 1):
                lrus.append('{}{}'.format(results[0][0], str(i)))
            lrusAlias = lrus
        else:
            lrus = [measurement['lru']]
            lrusAlias = [measurement['lru_alias']]

        # Iterate over all abms, lrus and mons
        for indexAbm, abm in enumerate(abms):
            for indexLru, lru in enumerate(lrus):
                plotData = {
                    'abm': abm,
                    'lru': lru,
                    'monitor': measurement['mon']
                }

                data = fetcher.getData(plotData, daysBack=daysBack, strDateEnd=dateEnd)

                # Remove microseconds part of timestamp to reduce storage
                # Alternatively use: lambda t: t.strftime('%Y-%m-%d %H:%M:%S')
                # data.index = data.index.map(lambda x: x.replace(microsecond=0))

                dataset = []
                if len(data) > 0:
                    for row in data.itertuples():
                        dataset.append(
                            cfg.MEASUREMENT_BASE.format(
                                measurement=measurement['mon_alias'],
                                abm=abmsAlias[indexAbm],
                                lru=lrusAlias[indexLru],
                                value=row.col1,
                                timestamp=int(row.Index.timestamp())
                            )                   # ^--- for ns precision, use: int(row.Index.to_datetime64()
                        )

                # Push measurements to database in batches of TIMESERIES_DB_BATCH_SIZE records
                if dataset:
                    lenDataset = len(dataset)
                    totalChunks = math.ceil(lenDataset / cfg.TIMESERIES_DB_BATCH_SIZE)
                    for i, datasetChunk in enumerate(chunks(dataset, cfg.TIMESERIES_DB_BATCH_SIZE), 1):
                        measurements = '\n'.join(datasetChunk)
                        # Store measurements in timeseries database
                        if not DEBUG:
                            try:
                                response = requests.post(cfg.TIMESERIES_DB_API_URL.format(rp=measurement.get('retention_policy', 'autogen')), data=measurements)
                                if response.status_code == cfg.REQUEST_STATUS_CODE_OK:
                                    logger.info(f"Measurement {measurement['mon_alias']},{abmsAlias[indexAbm]},{lrusAlias[indexLru]} correctly " + \
                                                f"stored in DATABASE {cfg.TIMESERIES_DB_NAME} RP {measurement['retention_policy']}: {len(datasetChunk)}/{lenDataset} ({i}/{totalChunks}) records, timestamp {datetime.now().isoformat()}")
                                    pass
                                else:
                                    logger.error('Error storing measurements in database')
                            except:
                                logger.exception('Error storing measurements in timeseries database')
                        else:
                            logger.debug('Simulation mode:')
                            logger.debug(measurements)

        # Set cache key for expiration calculation
        queryHrs = measurement.get('query_hrs')
        if queryHrs:
            cache.setKeyExpire(
                '{}:{}:{}'.format(measurement['abm'], measurement['lru'], measurement['mon']),
                datetime.now().isoformat(),
                queryHrs * 60 * 60,
            )

    logger.info('Finished mon-to-influx data collection.')

if __name__ == '__main__':
    sys.exit(typer.run(main))
