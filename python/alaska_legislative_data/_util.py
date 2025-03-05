import datetime

DEFAULT_DB_PATH = "ak_leg.duckdb"


def current_leg_num_approx(current_year: int | None = None) -> int:
    """Estimate the current legislature number.

    current_leg_num_approx(2021) == 32
    current_leg_num_approx(2022) == 32
    current_leg_num_approx(2023) == 33
    current_leg_num_approx(2024) == 33
    """
    if current_year is None:
        current_year = datetime.datetime.now().year
    return (current_year - 2023) // 2 + 33


assert current_leg_num_approx(2021) == 32, current_leg_num_approx(2021)
assert current_leg_num_approx(2022) == 32, current_leg_num_approx(2022)
assert current_leg_num_approx(2023) == 33, current_leg_num_approx(2023)
assert current_leg_num_approx(2024) == 33, current_leg_num_approx(2024)
assert current_leg_num_approx(2025) == 34, current_leg_num_approx(2025)
assert current_leg_num_approx(2026) == 34, current_leg_num_approx(2026)
