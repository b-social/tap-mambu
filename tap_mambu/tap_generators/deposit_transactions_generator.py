from .multithreaded_bookmark_generator import MultithreadedBookmarkGenerator
from ..helpers import get_bookmark
from ..helpers.datetime_utils import datetime_to_utc_str, str_to_localized_datetime, add_days, amend_timestamp


class DepositTransactionsGenerator(MultithreadedBookmarkGenerator):
    def _init_config(self):
        super()._init_config()
        self.max_threads = 5

    def _init_endpoint_config(self):
        super(DepositTransactionsGenerator, self)._init_endpoint_config()
        self.endpoint_path = "deposits/transactions:search"
        self.endpoint_bookmark_field = "creationDate"
        # self.endpoint_sorting_criteria["field"] = "id"
        self.endpoint_sorting_criteria = None
        self.dt_start_date = str_to_localized_datetime(
                    get_bookmark(self.state, self.stream_name, self.sub_type, self.start_date))
        self.dt_end_date = add_days(self.dt_start_date, self.days_to_process)
        self.endpoint_filter_criteria = [
            {
                "field": "creationDate",
                "operator": "BETWEEN",
                "value": amend_timestamp(self.dt_start_date.isoformat()),
                "secondValue" : amend_timestamp(self.dt_end_date.isoformat())
            }
        ]

    def prepare_batch_params(self):
        super(DepositTransactionsGenerator, self).prepare_batch_params()
        # self.endpoint_filter_criteria[0]["value"] = datetime_to_utc_str(self.endpoint_intermediary_bookmark_value)
        self.endpoint_filter_criteria[0]["value"] = amend_timestamp(self.endpoint_intermediary_bookmark_value.isoformat())
