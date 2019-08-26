from datetime import datetime
# import urllib3
from . import MonPlotDefinitions as MonPlotDefs

BASE_URL = MonPlotDefs.BASE_URL

#---------------------------------------
# Construct the monitor data service URL for any given date, ABM, LRU and monitor point
#   @param string date Date in yyyy-mm-dd hh:mm:ss or yyyy-mm-dd format, e.g. '2013-09-19 03:06:10'
#   @param string abm ABM name, e.g. 'PM02', 'AOSTiming'
#   @param string lru LRU name, e.g. 'IFProc0', 'Mount'
#   @param string mon Monitor name as it appears in monitor data service pages, e.g., 'ANTENNA_TEMPS', 'DATA_MONITOR_2'
#   @return string url for data download, e.g., http://monitordata.osf.alma.cl/index.php?dir=2013/09/2013-09-18/CONTROL_DA63_Mount/&download=ANTENNA_TEMPS.txt
def BuildMonPointUrl(date, abm, lru, mon):
    try:
        startDate = datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        startDate = datetime.strptime(date, '%Y-%m-%d')

    subSystemForUrl = 'CONTROL'

    # Treat (new) special cases for abms not part of the CONTROL subsystem (ACACORR, CORR)
    for specialSubsystem in MonPlotDefs.SPECIAL_SUBSYSTEMS:
        if abm.startswith(specialSubsystem[:-1] + '-'):
            abm = abm.replace(specialSubsystem[:-1] + '-', '')
            subSystemForUrl = specialSubsystem[:-1]

    url = BASE_URL + '/index.php?dir=%d/%02d/%d-%02d-%02d/%s_%s_%s/&download=%s.txt' % (startDate.year,
                                                                                        startDate.month,
                                                                                        startDate.year,
                                                                                        startDate.month,
                                                                                        startDate.day,
                                                                                        subSystemForUrl, abm, lru,
                                                                                        mon)
    return url

# #---------------------------------------
# # Read data from MonitorData website (one day of data)
# #   @param string url URL for data download, e.g., http://monitordata.osf.alma.cl/index.php?dir=2013/09/2013-09-18/CONTROL_DA63_Mount/&download=ANTENNA_TEMPS.txt
# #   @return string time_data One day timestamp/data string of data
# def ReadDataFromWeb(url):
#     try:
#         response = urllib.urlopen(url)
#     except urllib2.URLError:
#         print('ERROR: Could not fetch URL %s' % url)
#         return None
#     else:
#         time_data = response.read()
#         return time_data
#     finally:
#         response.close()
