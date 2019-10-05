"""
This module defines RetrieveData class, implementing routines to obtain monitor points,
generate monitor points plots and perform certain analysis based on data from MonitorData Service (http://monitordata.osf.alma.cl/EngLabs)
and eventually other data sources.

"""

import urllib3 as urllib2
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import tempfile
from os import path, makedirs, chmod
import csv
from math import *
import getpass
from time import time, sleep
import json

from . import UrlTools
from six.moves import range
import lib.BaseLogger as BaseLogger
import config.definitions as cfg

TIMING_TEST = False
MINIMUM_TIMESTAMP = datetime.strptime('2000-01-01T00:00:00', "%Y-%m-%dT%H:%M:%S")  # earlier dates considered corrupt
TEMP_DIR_BASE_NAME = 'monplot'  # typically located in /tmp
BASE_URL = 'http://monitordata.osf.alma.cl/EngLabs'

logger = BaseLogger.createLogger(__name__)
logger.addHandler(BaseLogger.getStdoutHandler())

#---------------------------------
class RetrieveData:
    """
    Class for retrieving monitoring data from Monitoring Data Base. Essential data integrity check is done on downloaded data.
    Downloaded data is temporarily cached on disk for increased performance (HDF compressed files). Pandas data analysis
    library is used.
    """

    #---------------------------------
    def __init__(self, plotData):
        self.logger = logger

        # if plotData is not a dictionary object, but a string, the argument is either the path to the json file
        # or a cPickle string representation of the dictionary
        if isinstance(plotData, str):
            # Read plot parameters from JSON def file
            if plotData.endswith('.json'):
                with open(plotData) as data_file:
                    plotData = json.load(data_file)
            # A 'cPickle' string representation of the plotData dictionary is directly passed as argument
            else:
                pass
                # plotData = cPickle.loads(plotData)

        self.plotData = plotData

        self.startDate = datetime.strptime(self.plotData['plot_from'], '%Y-%m-%d %H:%M:%S')

        if self.plotData['plot_to'] != 'now':
            self.endDate = datetime.strptime(self.plotData['plot_to'], '%Y-%m-%d %H:%M:%S')
        else:
            self.endDate = datetime.utcnow()

        if self.endDate < self.startDate:
            self.logger.error('End date must be later than start date')
            raise ValueError('End date must be later than start date')

        # # Temporary directory for data cache
        # if self.plotData.has_key('temp_dir') and self.plotData['temp_dir'] != '':
        #     self.tempDir = self.plotData['temp_dir']
        #     if not path.isdir(self.tempDir):
        #         print('Temporary dir %s does not exist. Creating...' % self.tempDir)
        #         self.createTempDirectory()
        # else:
        #     self.tempDir = None
        #     print('Temporary directory does not exist. Creating...')
        #     self.createTempDirectory()

        self.tempDir = '/dev/null'

        # Determine if globally use cached data files on disk (reading o writing them)
        # if self.plotData.has_key('no_cache'):
        if 'no_cache' in self.plotData:
            if self.plotData['no_cache']:
                self.noCache = True
            else:
                self.noCache = False
        else:
            self.noCache = False

        # if not self.plotData.has_key('data') or (self.plotData.has_key('data') and len(self.plotData['data']) == 0):
        if 'data' not in self.plotData or ('data' in self.plotData and len(self.plotData['data']) == 0):
            self.logger.error('No signals to plot were provided')
            raise ValueError('No signals to plot were provided')

        if 'save_csv_dir' in self.plotData:
            self.saveCsvFilesDir = self.plotData['save_csv_dir']
        else:
            self.saveCsvFilesDir = None

        self.numMonFiles = None
    #---------------------------------------
    def downloadDailyData(self, currDate, abm, lru, monitor):
        # Read data from MonitorData website
        url = UrlTools.BuildMonPointUrl(currDate.strftime('%Y-%m-%d'), abm, lru, monitor)
        self.logger.info('Reading data from %s...' % url)
        # sys.stdout.flush()

        raw = pd.DataFrame()
        ex = None
        try:
            # Read remote CSV file directly to a Pandas DataFrame structure
            raw = pd.read_csv(url, sep=' ', header=None, engine='c', prefix='col')
        #except pd.cparser.CParserError as ex:
        #    print('Skipping malformed file %s...' % url)
        except (ValueError, pd.io.common.EmptyDataError) as ex:
            self.logger.error('Skipping empty file %s...' % url)
        except urllib2.URLError as ex:
            self.logger.error('ERROR: Could not fetch URL %s.  Trying for a second time...' % url)
            sleep(1)
            try:
                raw = pd.read_csv(url, sep=' ', header=None, engine='c', prefix='col')
            #except pd.parser.CParserError as ex:
            #    print('Skipping malformed file %s...' % url)
            except ValueError as ex:
                self.logger.error('Skipping empty file %s...' % url)
            except urllib2.URLError as ex:
                self.logger.error('ERROR: Gave up trying to fetch URL %s' % url)
                sleep(1)
            except:
                self.logger.exception('Skipping file %s...' % url)
        except:
            self.logger.exception('Skipping file %s...' % url)
        # finally:
        #     sys.stdout.flush()

        # return raw, ex
        return raw, None

    #---------------------------------------
    def retrieve(self):
        # Determine how many days (number of different monitor files) are needed
        self.numMonFiles = (self.endDate.date() - self.startDate.date()).days + 1

        # Indicate if at least one signal data could be correctly obtained
        noSignals = True

        # For each signal to download/plot
        for idx, plotter in enumerate(self.plotData['data']):
            # Initial time for calculation of routine elapsed time
            fTime = time()

            if 'ignore' in plotter:
                if plotter['ignore']:
                    continue

            # "virtual" signals are not downloaded
            if plotter['abm'] == 'virtual':
                continue

            # Per signal no-cache setting.  Only observed when global no_cache is False(0)
            if 'no_cache' in plotter:
                if plotter['no_cache']:
                    signalNoCache = True
                else:
                    signalNoCache = False
            else:
                signalNoCache = False


            dataCol = int(plotter['data_column'])
            self.logger.info('\nProcessing %s / %s / %s / col%d' % (plotter['abm'], plotter['lru'], plotter['monitor'], dataCol))

            # If present, used cached data files for current data set
            dataFileName = '%s_%s_%s_%s_%ddays.hdf' % (plotter['abm'], plotter['lru'], plotter['monitor'],
                                                       self.startDate.strftime('%Y%m%d'), self.numMonFiles)
            if not self.noCache and not signalNoCache and path.isfile(path.join(self.tempDir, dataFileName)):
                self.logger.info('Found cached data file %s' % path.join(self.tempDir, dataFileName))
                data = pd.read_hdf(path.join(self.tempDir, dataFileName), 'data')
                noSignals = False
            else:
                data = pd.DataFrame()

                # Download all monitor files needed to cover the required date range (1 data file per day)
                for currDate in map(lambda x: self.startDate + timedelta(days=x), range(self.numMonFiles)):
                    usingAlternativeMonitor = False

                    raw, ex = self.downloadDailyData(currDate, plotter['abm'], plotter['lru'], plotter['monitor'])

                    if ex is not None:
                        # Malformed CSV file, just skip file
                        if type(ex) is pd.parser.CParserError:
                            continue
                        # Empty file, attempt alternative lru or monitor point name, if available
                        elif type(ex) in [ValueError, pd.io.common.EmptyDataError]:
                            if 'monitor_alternative' in plotter and plotter['monitor_alternative'] != '':
                                self.logger.info('Attempting alternative monitor point %s' % plotter['monitor_alternative'])

                                if not isinstance(plotter['monitor_alternative'], list):
                                    raw, ex = self.downloadDailyData(currDate, plotter['abm'], plotter['lru'], plotter['monitor_alternative'])
                                    if ex is not None:
                                        continue
                                    else:
                                        usingAlternativeMonitor = True
                                # Experimental: construct multi-column Pandas dataframe from alternative monitor point list
                                else:
                                    rawList = [None] * len(plotter['monitor_alternative'])
                                    for i, monitorAlternative in enumerate(plotter['monitor_alternative']):
                                        rawList[i], ex = self.downloadDailyData(currDate, plotter['abm'], plotter['lru'], monitorAlternative)
                                        if ex is not None:
                                            rawList[i] = None

                                    if rawList[0] is None:
                                        continue

                                    raw = rawList[0]
                                    for i, rawAlternative in enumerate(rawList):
                                        if i == 0:
                                            continue
                                        if rawAlternative is not None:
                                            raw['col%d' % (i + 1)] = rawAlternative.iloc[:, -1:]
                                        else:
                                            raw['col%d' % (i + 1)] = np.nan
                                    usingAlternativeMonitor = True
                            elif 'lru_alternative' in plotter and plotter['lru_alternative']:
                                self.logger.info('Attempting alternative lru name %s' % plotter['lru_alternative'])
                                raw, ex = self.downloadDailyData(currDate, plotter['abm'], plotter['lru_alternative'], plotter['monitor'])
                                if ex is not None:
                                    self.logger.info('No data found on alternative lru name %s. Skipping...' % plotter['lru_alternative'])
                                    continue
                        # Failed URL fetch, gave up trying
                        elif type(ex) is urllib2.URLError:
                            continue

                    data = data.append(raw, ignore_index=True)
                    dataCol = int(plotter['data_column']) if not usingAlternativeMonitor \
                                                          else int(plotter['data_column_alternative'])

                    # # Update data dictionary with the resulting "data column" used
                    # self.plotData['data'][idx]['data_column_definitive'] = dataCol

                # Validate requested column exists in dataframe
                if not 'col%d' % dataCol in data.columns:
                    self.logger.info('No column %d found in data. Skipping signal.' % dataCol)
                    continue

                noSignals = False

                initLen = len(data)
                self.logger.info('Processing and filtering data. %d monitor points. Please wait...' % initLen)
                # sys.stdout.flush()

                # Convert timestamp column to datetime format
                data.col0 = pd.to_datetime(data.col0, format="%Y-%m-%dT%H:%M:%S.%f", errors='coerce')

                # Validate minimum date
                data.loc[data.col0 < MINIMUM_TIMESTAMP, 'col0'] = pd.NaT

                # Set timestamp column as dataframe index
                data.set_index('col0', inplace=True)

                # Remove corrupt timestamp rows
                data.drop(pd.NaT, inplace=True, errors='ignore')

                # Remove rows with non-numeric data
                data = data.apply(lambda x: pd.to_numeric(x, errors='coerce'))
                data.dropna(axis='index', how='all', inplace=True)

                self.logger.info('{} / {} / {} Data rows removed: {}'.format(plotter['abm'], plotter['lru'], plotter['monitor'], (initLen - len(data))))
                # sys.stdout.flush()

                # # Keep a temporary copy of downloaded and filtered data
                # print('\nDumping retrieved filtered data to temporary file...')
                # sys.stdout.flush()

                # data.to_hdf(path.join(self.tempDir, dataFileName), 'data', complevel=9, complib='zlib')
                # print('Data is ready')
                # print(data)
                return data
                # print('Saved temporary file %s' % path.join(self.tempDir, dataFileName))
            # end initial data preparation for current signal

            # print
            # data.info()
            # # print(data.head())
            # print
