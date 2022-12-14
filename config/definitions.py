DEBUG = False

# Antennas list for iteration
CMs = ['CM%02d' % (x + 1) for x in range(12)]
DAs = ['DA%02d' % (x + 41) for x in range(25)]
DVs = ['DV%02d' % (x + 1) for x in range(25)]
PMs = ['PM%02d' % (x + 1) for x in range(4)]
allABMs = CMs + DAs + DVs + PMs

TIMESERIES_DB_HOST = '172.17.0.1'
TIMESERIES_DB_PORT = '8086'
TIMESERIES_DB_NAME = 'testdb'
TIMESERIES_DB_API_URL = 'http://{host}:{port}/write?db={db}&rp={{rp}}&precision=s'.format(
                                                                                         host=TIMESERIES_DB_HOST,
                                                                                         port=TIMESERIES_DB_PORT,
                                                                                         db=TIMESERIES_DB_NAME
                                                                                       )
TIMESERIES_DB_BATCH_SIZE = 10000
MEASUREMENT_BASE = '{measurement},abm={abm},lru={lru} value={value:.3f} {timestamp}'
REQUEST_STATUS_CODE_OK = 204

# Cache DB definitions
CACHE_HOST='172.17.0.1'