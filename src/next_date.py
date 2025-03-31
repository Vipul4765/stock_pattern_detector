from datetime import datetime, timedelta

def increment_date(date_str):
    """
    Increment the given date by one day.

    Parameters:
    date_str (str): Date in DDMMYYYY format.

    Returns:
    str: New date incremented by one day, in DDMMYYYY format.
    """
    date_obj = datetime.strptime(date_str, "%d%m%Y")
    new_date_obj = date_obj + timedelta(days=1)
    new_date_str = new_date_obj.strftime("%d%m%Y")
    return new_date_str

def generate_date_range(start_date, end_date):
    """
    Generate a list of dates between the start_date and end_date.

    Parameters:
    start_date (str): Start date in DDMMYYYY format.
    end_date (str): End date in DDMMYYYY format.

    Returns:
    list: List of dates between start_date and end_date in DDMMYYYY format.
    """
    dates = []
    current_date = start_date

    # Convert end_date to a datetime object for comparison
    end_date_obj = datetime.strptime(end_date, "%d%m%Y")

    while datetime.strptime(current_date, "%d%m%Y") <= end_date_obj:
        dates.append(current_date)
        current_date = increment_date(current_date)

    return dates
