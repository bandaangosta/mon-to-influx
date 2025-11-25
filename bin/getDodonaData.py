#!/usr/bin/env python3

"""
ALMA monitoring database (text-based, a.k.a. Dodona) exporter
"""

# import pdb; pdb.set_trace()

import os
import sys

# Adds application root to path
PATH_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir)
)
if not PATH_ROOT in sys.path:
    sys.path.insert(1, PATH_ROOT)

# # Activate Python virtual environment containing all needed libraries and dependencies
# if os.path.exists(os.path.join(PATH_ROOT, 'venv/bin/activate_this.py')):
#     activate_this = os.path.join(PATH_ROOT, 'venv', 'bin', 'activate_this.py')
# else:
#     print('Virtual environment not found')
#     raise SystemExit
# exec(open(activate_this).read(), {'__file__': activate_this})

from datetime import datetime
import typer
from lib.DataFetch import DataFetch
import lib.BaseLogger as BaseLogger
import lib.DateTools as DateTools

DEBUG = True


def main(
    daysBack: int = typer.Option(
        help="Number of days back from now to retrieve", default=1, show_default=True
    ),
    dateEnd: str = typer.Option(
        help="Instead of now, retrieve data back from this moment. Format is yyyy-mm-dd hh:mm:ss",
        default=DateTools.dateToStr(datetime.now()),
    ),
    subsystem: str = typer.Option(help="Typically, CONTROL", default="CONTROL"),
    abm: str = typer.Option(
        help="E2C name of the control computer to retrieve data from. E.g., DV01",
        default="",
    ),
    lru: str = typer.Option(
        help="LRU that generates the monitor point. E.g., LORR", default=""
    ),
    monitor: str = typer.Option(
        help="Monitor point name. E.g., AMBIENT_TEMPERATURE", default=""
    ),
):
    """
    Get monitor data from ALMA monitoring database (Dodona) and return a pandas dataframe.
    """

    logger = BaseLogger.createLogger("dodona-export", rotator=True)
    if DEBUG:
        logger.addHandler(BaseLogger.getStdoutHandler())

    logger.info(f"Started dodona-export data collection.")

    # Data collector from ALMA monitoring database (based on MonitorPlotter application)
    fetcher = DataFetch()

    logger.info(f"Obtaining data for {daysBack} days back from {dateEnd}")

    plotData = {"abm": abm, "lru": lru, "monitor": monitor}

    data = fetcher.getData(plotData, daysBack=daysBack, strDateEnd=dateEnd)
    data.to_csv("dodona_export.csv")

    # Remove microseconds part of timestamp to reduce storage
    # Alternatively use: lambda t: t.strftime('%Y-%m-%d %H:%M:%S')
    # data.index = data.index.map(lambda x: x.replace(microsecond=0))

    logger.info("Finished dodona-export data collection to file dodona_export.csv")


if __name__ == "__main__":
    sys.exit(typer.run(main))
