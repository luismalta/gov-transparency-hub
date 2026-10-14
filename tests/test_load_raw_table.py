import dlt
import duckdb
import pandas as pd
import pytest

from gov_transparency_hub.assets.raw_load_assets import (
    RawTableConfig,
    load_raw_table,
    prepare_raw_dataframe,
)

LICITACOES = RawTableConfig(
    "licitacoes", ("municipio", "unidade_gestora", "numero_do_processo")
)
RECEITA = RawTableConfig(
    "receita",
    ("municipio", "unidade_gestora", "ano_referencia"),
    strategy="delete-insert",
)


@pytest.fixture
def duckdb_path(tmp_path):
    return str(tmp_path / "warehouse.duckdb")


def _load(df, config, duckdb_path, tmp_path, city="tiradentes", year="2025"):
    prepared = prepare_raw_dataframe(df, config, city, year)
    return load_raw_table(
        prepared,
        config,
        dlt.destinations.duckdb(duckdb_path),
        str(tmp_path / "pipelines"),
    )


def _query(duckdb_path, sql):
    with duckdb.connect(duckdb_path) as conn:
        return conn.sql(sql).df()


def _licitacoes(valor):
    return pd.DataFrame(
        {
            "numeroDoProcesso": ["1/2025", "2/2025"],
            "valor": [valor, 5.0],
            "unidade_gestora": ["1", "1"],
        }
    )


def test_upsert_is_idempotent(duckdb_path, tmp_path):
    _load(_licitacoes(1.0), LICITACOES, duckdb_path, tmp_path)
    _load(_licitacoes(1.0), LICITACOES, duckdb_path, tmp_path)

    result = _query(duckdb_path, "select * from raw.licitacoes")

    assert len(result) == 2


def test_upsert_updates_changed_row(duckdb_path, tmp_path):
    _load(_licitacoes(1.0), LICITACOES, duckdb_path, tmp_path)
    _load(_licitacoes(9.0), LICITACOES, duckdb_path, tmp_path)

    result = _query(
        duckdb_path,
        "select valor from raw.licitacoes where numero_do_processo = '1/2025'",
    )

    assert result["valor"].tolist() == ["9.0"]


def _receita(unidade_gestora, valores):
    return pd.DataFrame(
        {
            "id": ["11125001"] * len(valores),
            "data": ["2025-01-02"] * len(valores),
            "valor": valores,
            "unidade_gestora": [unidade_gestora] * len(valores),
        }
    )


def test_delete_insert_preserves_identical_rows(duckdb_path, tmp_path):
    _load(_receita("1", [10.0, 10.0]), RECEITA, duckdb_path, tmp_path)

    result = _query(duckdb_path, "select * from raw.receita")

    assert len(result) == 2


def test_delete_insert_is_idempotent(duckdb_path, tmp_path):
    _load(_receita("1", [10.0, 10.0]), RECEITA, duckdb_path, tmp_path)
    _load(_receita("1", [10.0, 10.0]), RECEITA, duckdb_path, tmp_path)

    result = _query(duckdb_path, "select * from raw.receita")

    assert len(result) == 2


def test_delete_insert_replaces_only_its_slice(duckdb_path, tmp_path):
    _load(_receita("1", [10.0]), RECEITA, duckdb_path, tmp_path)
    _load(_receita("2", [20.0]), RECEITA, duckdb_path, tmp_path)
    _load(_receita("1", [10.0]), RECEITA, duckdb_path, tmp_path, city="prados")
    _load(_receita("1", [11.0, 12.0]), RECEITA, duckdb_path, tmp_path)

    result = _query(
        duckdb_path,
        "select municipio, unidade_gestora, valor from raw.receita "
        "order by municipio, unidade_gestora, valor",
    )

    assert result.values.tolist() == [
        ["prados", "1", "10.0"],
        ["tiradentes", "1", "11.0"],
        ["tiradentes", "1", "12.0"],
        ["tiradentes", "2", "20.0"],
    ]


def test_returns_load_id(duckdb_path, tmp_path):
    load_id = _load(_licitacoes(1.0), LICITACOES, duckdb_path, tmp_path)

    assert isinstance(load_id, str) and load_id


def test_float_after_int_partition_is_not_truncated(duckdb_path, tmp_path):
    # Production runs get a fresh pipelines_dir each time, so the column type
    # chosen by the first partition is what later partitions meet.
    first = _licitacoes(1).assign(valor=[1, 5])
    later = _licitacoes(1.5)
    for run, df in enumerate([first, later]):
        load_raw_table(
            prepare_raw_dataframe(df, LICITACOES, "tiradentes", "2025"),
            LICITACOES,
            dlt.destinations.duckdb(duckdb_path),
            str(tmp_path / f"pipelines_{run}"),
        )

    result = _query(
        duckdb_path,
        "select valor from raw.licitacoes where numero_do_processo = '1/2025'",
    )

    assert result["valor"].tolist() == ["1.5"]
