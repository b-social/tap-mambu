from abc import ABC
from typing import List
from singer import utils, get_logger

from ..helpers import transform_json
from ..helpers.hashable_dict import HashableDict
from ..helpers.perf_metrics import PerformanceMetrics

LOGGER = get_logger()


class TapGenerator(ABC):
    def __init__(self, stream_name, client, config, state, sub_type):
        self.stream_name = stream_name
        self.client = client
        self.config = config
        self.state = state
        self.sub_type = sub_type

        # Define parameters inside init
        self.params = dict()
        self.time_extracted = 0
        self.offset = 0

        # Initialize parameters
        self._init_config()
        self._init_endpoint_config()
        self._init_endpoint_body()
        self._init_buffers()
        self._init_params()

    def _init_config(self):
        self.start_date = self.config.get('start_date')
        self.days_to_process = int(self.config.get('days_to_process', 1))

    def _init_endpoint_config(self):
        self.endpoint_path = ""
        self.endpoint_api_version = "v2"
        self.endpoint_api_method = "POST"
        self.endpoint_params = {
            "detailsLevel": "FULL",
            "paginationDetails": "OFF"
        }
        self.endpoint_sorting_criteria = {
            "field": "encodedKey",
            "order": "ASC"
        }
        self.endpoint_filter_criteria = []
        self.endpoint_api_key_type = None
        self.endpoint_bookmark_field = ""

    def _init_endpoint_body(self):
        if self.endpoint_sorting_criteria != None:
            self.endpoint_body = {
                    "sortingCriteria": self.endpoint_sorting_criteria,
                    "filterCriteria": self.endpoint_filter_criteria
                    }
        else:
            self.endpoint_body = {
                    "filterCriteria": self.endpoint_filter_criteria
                    }

    def _init_buffers(self):
        self.buffer: List = list()

    def _init_params(self):
        self.time_extracted = None
        self.static_params = dict(self.endpoint_params)
        self.offset = 0
        self.limit = self.client.page_size
        self.params = self.static_params

    def _all_fetch_batch_steps(self):
        self.prepare_batch()
        raw_batch = self.fetch_batch()
        self.buffer = transform_json(raw_batch, self.stream_name)
        if not self.buffer:
            LOGGER.warning(f'(generator) Stream {self.stream_name} - NO TRANSFORMED DATA RESULTS')
        self.last_batch_size = len(self.buffer)

    def __iter__(self):
        self._all_fetch_batch_steps()
        return self

    def next(self):
        if not self.buffer:
            if self.last_batch_size < self.limit:
                raise StopIteration()
            self.offset += self.limit
            self._all_fetch_batch_steps()
            if not self.buffer:
                raise StopIteration()
        return self.buffer.pop(0)

    def __next__(self):
        # with PerformanceMetrics(metric_name="processor_wait"):
        return self.next()

    def prepare_batch(self):
        self.params = {
            "offset": self.offset,
            "limit": self.limit,
            **self.static_params
        }

    def transform_batch(self, batch):
        # Check if batch is list or dict and convert to Hashable dict accordingly
        if type(batch) == list:
            return list(map(HashableDict, batch))
        return HashableDict(batch)

    def fetch_batch(self):
        endpoint_querystring = '&'.join([f'{key}={value}' for (key, value) in self.params.items()])

        LOGGER.info(f'(generator) Stream {self.stream_name} - URL for {self.stream_name} ({self.endpoint_api_method}, '
                    f'{self.endpoint_api_version}): {self.client.base_url}/{self.endpoint_path}?{endpoint_querystring}')
        LOGGER.info(f'(generator) Stream {self.stream_name} - body = {self.endpoint_body}')

        with PerformanceMetrics(metric_name="generator"):
            response = self.client.request(
                method=self.endpoint_api_method,
                path=self.endpoint_path,
                version=self.endpoint_api_version,
                apikey_type=self.endpoint_api_key_type,
                params=endpoint_querystring,
                endpoint=self.stream_name,
                json=self.endpoint_body
            )

        self.time_extracted = utils.now()
        LOGGER.info(f'(generator) Stream {self.stream_name} - extracted records: {len(response)}')
        return self.transform_batch(response)
