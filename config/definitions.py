DEBUG = False

# Antennas list for iteration
CMs = ['CM%02d' % (x + 1) for x in range(12)]
DAs = ['DA%02d' % (x + 41) for x in range(25)]
DVs = ['DV%02d' % (x + 1) for x in range(25)]
PMs = ['PM%02d' % (x + 1) for x in range(4)]
allABMs = CMs + DAs + DVs + PMs

# Timeseries DB definitions
TIMESERIES_DB_HOST = '172.17.0.1'
TIMESERIES_DB_PORT = '8086'
TIMESERIES_DB_NAME = 'testdb'
TIMESERIES_DB_API_URL = 'http://{}:{}/write?db={}&precision=s'.format(TIMESERIES_DB_HOST, TIMESERIES_DB_PORT, TIMESERIES_DB_NAME)
TIMESERIES_DB_BATCH_SIZE = 10000
MEASUREMENT_BASE = '{measurement},abm={abm},lru={lru} value={value:.3f} {timestamp}'
REQUEST_STATUS_CODE_OK = 204
