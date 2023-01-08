from glue.utils.date_utils import DateUtils


class TestDateUtils:
    """
    Class for testing DateUtils
    """

    def test_get_date(self):
        """
            test_get_date
        """
        formatted_date_time, formatted_date_time_for_filename = DateUtils.get_formatted_current_date()

        assert formatted_date_time is not None
        assert formatted_date_time_for_filename is not None