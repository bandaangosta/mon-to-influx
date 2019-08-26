from datetime import datetime, timedelta

# ----------------------------------------
# Calculate a date deltaDays days in the past
def GetDateStringDelta(dateBase, deltaDays):
    datePastDelta = dateBase - timedelta(days=deltaDays)

    return datetime.strftime(datePastDelta, '%Y-%m-%d %H:%M:%S')

#---------------------------------------
def getMiddleDate(strStartDate, strEndDate):
    return getDateRangeFraction(strStartDate, strEndDate, 0.5)

#---------------------------------------
def getDateRangeFraction(strStartDate, strEndDate, dblFraction):
    dateStart = datetime.strptime(strStartDate, '%Y-%m-%d %H:%M:%S')
    dateEnd = datetime.strptime(strEndDate, '%Y-%m-%d %H:%M:%S')

    # Determine number of seconds in between
    numSecs = (dateEnd - dateStart).total_seconds()
    if numSecs > 1:
        dateResult = dateStart + timedelta(seconds=(int(numSecs * dblFraction)))
    else:
        dateResult = dateStart
    return datetime.strftime(dateResult, '%Y-%m-%d %H:%M:%S')

#---------------------------------------
# Validate that string yyyy-mm-dd hh:mm:ss or yyyy-mm-dd hh:mm:ss.f contains a valid date
def validateDate(strDate):
    try:
        date = datetime.strptime(strDate, '%Y-%m-%d %H:%M:%S')
        return date
    except ValueError:
        try:
            date = datetime.strptime(strDate, '%Y-%m-%d %H:%M:%S.%f')
            return date
        except:
            return False
    except:
        return False

#---------------------------------------
def dateToStr(dateTimeValue):
    try:
        date = datetime.strftime(dateTimeValue, '%Y-%m-%d %H:%M:%S')
        return date
    except:
        return False