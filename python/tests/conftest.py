import ibis
import pytest


def pytest_addoption(parser) -> None:
    parser.addoption(
        "--augmented-dir", action="store", default="./.ak-leg-data/augmented"
    )


@pytest.fixture
def bills(request) -> ibis.Table:
    aug_dir = request.config.getoption("--augmented-dir")
    return ibis.read_parquet(f"{aug_dir}/bills.parquet")
