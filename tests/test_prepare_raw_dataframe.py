import json

import pandas as pd
import pytest

from gov_transparency_hub.assets.raw_load_assets import (
    RawTableConfig,
    prepare_raw_dataframe,
)

LICITACOES = RawTableConfig(
    "licitacoes", ("municipio", "unidade_gestora", "numero_do_processo")
)
DESPESA = RawTableConfig(
    "despesa",
    ("municipio", "unidade_gestora", "ano_referencia", "empenho", "tipo"),
    flatten_column="dadosPrincipais",
)
RECEITA = RawTableConfig(
    "receita",
    ("municipio", "unidade_gestora", "ano_referencia"),
    strategy="delete-insert",
)


def test_normalizes_column_names_to_snake_case():
    df = pd.DataFrame(
        {"numeroDoProcesso": ["1/2025"], "unidade_gestora": ["1"]}
    )

    result = prepare_raw_dataframe(df, LICITACOES, "tiradentes", "2025")

    assert "numero_do_processo" in result.columns
    assert "numeroDoProcesso" not in result.columns


def test_adds_municipio_and_ano_referencia():
    df = pd.DataFrame(
        {"numeroDoProcesso": ["1/2025"], "unidade_gestora": ["1"]}
    )

    result = prepare_raw_dataframe(df, LICITACOES, "tiradentes", "2025")

    assert result["municipio"].tolist() == ["tiradentes"]
    assert result["ano_referencia"].tolist() == ["2025"]


def test_flattens_json_column_and_keeps_nested_values_as_json():
    dados = {
        "empenho": "00001-000",
        "tipo": "EO - Empenho Ordinário",
        "exercicio": "2024",
        "descontos": [{"valor": 1}],
    }
    df = pd.DataFrame(
        {
            "dadosPrincipais": [json.dumps(dados)],
            "notasFiscais": ["[]"],
            "unidade_gestora": ["1"],
        }
    )

    result = prepare_raw_dataframe(df, DESPESA, "tiradentes", "2025")

    assert "dados_principais" not in result.columns
    assert result.loc[0, "empenho"] == "00001-000"
    # source exercicio (restos a pagar) must not be overwritten
    assert result.loc[0, "exercicio"] == "2024"
    assert result.loc[0, "ano_referencia"] == "2025"
    assert json.loads(result.loc[0, "descontos"]) == [{"valor": 1}]
    assert result.loc[0, "notas_fiscais"] == "[]"


def test_flatten_tolerates_null_json_value():
    valid = json.dumps({"empenho": "1", "tipo": "EO"})
    df = pd.DataFrame(
        {"dadosPrincipais": [valid, None], "unidade_gestora": ["1", "1"]}
    )
    config = DESPESA._replace(
        key_columns=("municipio", "unidade_gestora"), strategy="delete-insert"
    )

    result = prepare_raw_dataframe(df, config, "tiradentes", "2025")

    assert len(result) == 2
    assert pd.isna(result.loc[1, "empenho"])


def test_drops_exact_duplicates_only_when_configured():
    df = pd.DataFrame({"id": ["1", "1"], "nome": ["A", "A"]})
    config = RawTableConfig(
        "fontes_de_recursos", ("municipio", "id"), drop_exact_duplicates=True
    )

    result = prepare_raw_dataframe(df, config, "tiradentes", "2025")

    assert len(result) == 1


def test_upsert_rejects_duplicate_keys():
    df = pd.DataFrame(
        {"numeroDoProcesso": ["1/2025", "1/2025"], "unidade_gestora": ["1", "1"]}
    )

    with pytest.raises(ValueError, match="duplicate key"):
        prepare_raw_dataframe(df, LICITACOES, "tiradentes", "2025")


def test_upsert_rejects_null_keys():
    df = pd.DataFrame(
        {"numeroDoProcesso": [None], "unidade_gestora": ["1"]}
    )

    with pytest.raises(ValueError, match="null key"):
        prepare_raw_dataframe(df, LICITACOES, "tiradentes", "2025")


def test_upsert_accepts_empty_string_in_key():
    # livro_de_lancamentos "SALDO INICIAL DO DIA" has numero == ""
    df = pd.DataFrame(
        {"data": ["2025-01-02"], "numero": [""], "unidade_gestora": ["1"]}
    )
    config = RawTableConfig(
        "livro_de_lancamentos",
        ("municipio", "unidade_gestora", "data", "numero"),
    )

    result = prepare_raw_dataframe(df, config, "tiradentes", "2025")

    assert len(result) == 1


def test_delete_insert_keeps_identical_rows():
    row = {"id": "11125001", "data": "2025-01-02", "valor": 10.0}
    df = pd.DataFrame([row, row]).assign(unidade_gestora="1")

    result = prepare_raw_dataframe(df, RECEITA, "tiradentes", "2025")

    assert len(result) == 2


def test_rejects_missing_key_column():
    df = pd.DataFrame({"numeroDoProcesso": ["1/2025"]})

    with pytest.raises(ValueError, match="unidade_gestora"):
        prepare_raw_dataframe(df, LICITACOES, "tiradentes", "2025")


def test_casts_source_columns_to_string_preserving_nulls():
    df = pd.DataFrame(
        {
            "id": ["01", "02"],
            "quantidade": [1, 2],
            "valor": [1.5, None],
            "unidade_gestora": ["1", "1"],
        }
    )
    config = RawTableConfig("funcoes", ("municipio", "unidade_gestora", "id"))

    result = prepare_raw_dataframe(df, config, "tiradentes", "2025")

    assert result["id"].tolist() == ["01", "02"]
    assert result["quantidade"].tolist() == ["1", "2"]
    assert result.loc[0, "valor"] == "1.5"
    assert pd.isna(result.loc[1, "valor"])


def test_delete_insert_rejects_null_keys():
    df = pd.DataFrame({"id": ["11125001"], "unidade_gestora": [None]})

    with pytest.raises(ValueError, match="null key"):
        prepare_raw_dataframe(df, RECEITA, "tiradentes", "2025")


@pytest.mark.parametrize("value", ["[1, 2]", "not json", 5])
def test_flatten_rejects_non_object_value(value):
    df = pd.DataFrame({"dadosPrincipais": [value], "unidade_gestora": ["1"]})

    with pytest.raises(ValueError, match="despesa: dadosPrincipais"):
        prepare_raw_dataframe(df, DESPESA, "tiradentes", "2025")
