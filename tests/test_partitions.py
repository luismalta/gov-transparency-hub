import pytest

from gov_transparency_hub.partitions import (
    city_partition,
    daily_city_partition,
    monthly_city_partition,
    partition_scope,
    partitions_def_for,
    yearly_city_partition,
)


@pytest.mark.parametrize(
    ("param_names", "expected"),
    [
        (("exercicio", "unidade_gestora", "data_do_empenho_inicial", "data_do_empenho_final"), daily_city_partition),
        (("unidade_gestora", "exercicio_de_criacao", "mes_de_criacao"), monthly_city_partition),
        (("exercicio", "unidade_gestora"), yearly_city_partition),
        (("exercicio",), yearly_city_partition),
        ((), city_partition),
    ],
)
def test_partitions_follow_the_endpoint_granularity(param_names, expected):
    assert partitions_def_for(param_names) == expected


@pytest.mark.parametrize(
    ("partitions_def", "key", "expected"),
    [
        (daily_city_partition, "tiradentes|2025-01-02", ("tiradentes", "2025-01-02")),
        (monthly_city_partition, "tiradentes|2025-01", ("tiradentes", "2025-01")),
        (yearly_city_partition, "tiradentes|2025", ("tiradentes", "2025")),
        (city_partition, "tiradentes", ("tiradentes", None)),
    ],
)
def test_partition_scope_splits_city_and_period(partitions_def, key, expected):
    assert partition_scope(partitions_def, key) == expected


def test_new_partitions_use_short_period_keys():
    assert "tiradentes|2025-01" in monthly_city_partition.get_partition_keys()
    assert "tiradentes|2025" in yearly_city_partition.get_partition_keys()


def test_current_year_and_month_can_be_materialized():
    # Annual/monthly endpoints are refreshed while their period is open;
    # without an end offset Dagster only exposes completed windows.
    from datetime import UTC, datetime

    today = datetime.now(tz=UTC).date()
    years = {x.name: x.partitions_def for x in yearly_city_partition.partitions_defs}
    months = {x.name: x.partitions_def for x in monthly_city_partition.partitions_defs}

    assert str(today.year) in years["year"].get_partition_keys()
    assert today.strftime("%Y-%m") in months["month"].get_partition_keys()
