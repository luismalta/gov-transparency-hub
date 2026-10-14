import pandas as pd

from gov_transparency_hub.assets.raw_load_assets import (
    RawTableConfig,
    combine_partition_frames,
)

FUNCOES = RawTableConfig(
    "funcoes", ("municipio", "unidade_gestora", "ano_referencia", "id")
)
RECEITA = RawTableConfig(
    "receita",
    ("municipio", "unidade_gestora", "ano_referencia"),
    strategy="delete-insert",
)


def _frame(**columns):
    return pd.DataFrame(columns).astype("string")


def test_upsert_latest_partition_wins():
    older = _frame(municipio=["m"], unidade_gestora=["1"], ano_referencia=["2025"], id=["01"], nome=["OLD"])
    newer = _frame(municipio=["m"], unidade_gestora=["1"], ano_referencia=["2025"], id=["01"], nome=["NEW"])

    result = combine_partition_frames(
        [("2025-01-03", newer), ("2025-01-02", older)], FUNCOES
    )

    assert result["nome"].tolist() == ["NEW"]


def test_disjoint_keys_from_different_partitions_are_all_kept():
    a = _frame(municipio=["a"], unidade_gestora=["1"], ano_referencia=["2025"], id=["01"], nome=["X"])
    b = _frame(municipio=["b"], unidade_gestora=["1"], ano_referencia=["2025"], id=["01"], nome=["Y"])

    result = combine_partition_frames([("2025-01-02", a), ("2025-01-02", b)], FUNCOES)

    assert sorted(result["municipio"].tolist()) == ["a", "b"]


def test_delete_insert_latest_slice_wins():
    # Each partition holds the whole year of the UG: the latest one replaces it.
    older = _frame(
        municipio=["m", "m", "m"], unidade_gestora=["1", "1", "1"],
        ano_referencia=["2025", "2025", "2025"], valor=["1", "2", "3"],
    )
    newer = _frame(
        municipio=["m", "m"], unidade_gestora=["1", "1"],
        ano_referencia=["2025", "2025"], valor=["10", "10"],
    )

    result = combine_partition_frames(
        [("2025-01-02", older), ("2025-01-03", newer)], RECEITA
    )

    # Identical rows within the winning partition stay (equal collections).
    assert result["valor"].tolist() == ["10", "10"]


def test_partition_tag_is_not_in_the_output():
    frame = _frame(municipio=["m"], unidade_gestora=["1"], ano_referencia=["2025"], id=["01"])

    result = combine_partition_frames([("2025-01-02", frame)], FUNCOES)

    assert list(result.columns) == list(frame.columns)
