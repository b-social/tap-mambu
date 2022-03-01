import inspect
import os

from mock import MagicMock

from ..constants import config_json
from ..helpers import GeneratorMock

FIXTURES_PATH = f"{os.path.dirname(os.path.abspath(inspect.stack()[0][1]))}/Fixtures"
print(FIXTURES_PATH)

def test_deposit_accounts_processor():
    from singer.catalog import Catalog
    catalog = Catalog.load(f"{FIXTURES_PATH}/processor_catalog.json")
    client_mock = MagicMock()

    from tap_mambu.tap_mambu_refactor.tap_processors.deposit_accounts_processor import DepositAccountsProcessor
    processor = DepositAccountsProcessor(catalog=catalog,
                                         stream_name="loan_accounts",
                                         client=client_mock,
                                         config=config_json,
                                         state={'currently_syncing': 'loan_accounts'},
                                         sub_type="self",
                                         generators=[GeneratorMock([])])

    assert processor.endpoint_deduplication_key == "id"
    assert processor.endpoint_child_streams == ["cards"]
