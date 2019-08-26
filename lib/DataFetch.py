import glob, re, os
# import dataiku
from datetime import datetime
# import matplotlib
# matplotlib.use('Agg')

# import HwMonitoring.PlotMonitor as PlotMonitor
# import HwMonitoring.DateTools as DateTools
import lib.PlotMonitor as PlotMonitor
import lib.DateTools as DateTools

class DataFetch():
    def __init__(self):
        pass
        # # Get current project cache folder data
        # projectConfig = dataiku.get_custom_variables()
        # cacheFolder = dataiku.Folder(projectConfig["CACHE_FOLDER_ID"])
        # cacheFolderInfo = cacheFolder.get_info()
        # self.cacheFolderAbsPath = cacheFolderInfo['path']

    # def deletePreviousData(self):
    #     # Delete previous data
    #     for filename in glob.glob(os.path.join(self.cacheFolderAbsPath, '*.hdf')):
    #         print('Deleting {}'.format(filename))
    #         os.remove(filename)

    def prepareMetadata(self, plotData, daysBack, strDateEnd=None):
        if strDateEnd is None:
            now = datetime.now()
            strEndDate = DateTools.dateToStr(now)
            endDate = now
        else:
            strEndDate = strDateEnd
            endDate = DateTools.validateDate(strEndDate)

        strStartDate = DateTools.GetDateStringDelta(endDate, daysBack)

        plotData = {
                    'data': [
                             {'abm': plotData['abm'],
                              'data_column': plotData.get('data_column', 1),
                              'data_multiplier': '1',
                              'data_offset': '0',
                              'formula': '',
                              'legend': '',
                              'linestyle': '. r',
                              'lru': plotData['lru'],
                              'monitor': plotData['monitor'],
                              'rolling_function': '',
                              'rolling_samples': '',
                              'y_axis': 'primary'
                             }
                           ],
                    'date_format_string': '%b/%d %H:%M',
                    # 'file_name' : os.path.join(self.cacheFolderAbsPath, '{}_{}_{}.png'.format(plotData['abm'], plotData['lru'], plotData['monitor'])),
                    'id_plot': None,
                    'julian_offset': 0,
                    'plot_from': strStartDate,
                    'plot_regular_date': True,
                    'plot_to': strEndDate,
                    'save_csv_dir': None,
                    'secondary_is_subplot': True,
                    'show_grid': True,
                    'show_grid_secondary': True,
                    # 'temp_dir': self.cacheFolderAbsPath,
                    'temp_dir': '/dev/null',
                    'title': 'Title',
                    'use_log_scale': False,
                    'use_log_scale_secondary': False,
                    'use_sci_notation': False,
                    'use_sci_notation_secondary': False,
                    'x_label': 'Time [UTC]',
                    'y_label': 'Variable [units]',
                    'y_label_secondary': '',
                    'y_max': '',
                    'y_max_secondary': '',
                    'y_min': '',
                    'y_min_secondary': ''
                   }
        self.plotMetadata = plotData

    def getData(self, plotData, daysBack=1, strDateEnd=None):
        self.prepareMetadata(plotData, daysBack, strDateEnd)

        # Retrieve data and generate plot
        pm = PlotMonitor.RetrieveData(self.plotMetadata)
        return pm.retrieve()
        #pm = PlotMonitor.PlotData(plotData)
        #pm.plot()

        # self.renameFiles()

    # def renameFiles(self):
    #     # Remove timestamp from file name
    #     for filename in glob.glob(os.path.join(self.cacheFolderAbsPath, '*.hdf')):
    #         print(filename)
    #         new_name = re.sub('(.*)_\d{8}_.*\.hdf', r'\1.hdf', filename)
    #         os.rename(filename, new_name)