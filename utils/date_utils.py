import datetime


class DateUtils:
    """
    class containing date util functions
    """

    @staticmethod
    def get_formatted_current_date():
        """
        Static Function to get date partition path for s3
        :return: date partition path for s3
        :return: formatted datetime to be appended to filename
        """
        current_date = datetime.datetime.utcnow()
        formatted_date_time = current_date.strftime('%Y/%m/%d/%H')
        formatted_date_time_for_filename = current_date.strftime("%Y%m%d%H%M%S")
        return formatted_date_time, formatted_date_time_for_filename
