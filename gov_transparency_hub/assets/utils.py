import hashlib
import pandas as pd


def format_object_name(asset_group, city_name, date):
    return f"{asset_group}/{city_name}-{asset_group}-{date}"

def create_surrogate_key(
    df: pd.DataFrame,
    hash_columns: list,
    prefix_columns: list,
    hash_func: str = 'sha256'
) -> pd.Series:
    """
    Creates a surrogate key for a DataFrame based on specified columns.
    
    Args:
        df (pd.DataFrame): DataFrame de entrada.
        hash_columns (list): Colunas usadas para formar o conteúdo do hash.
        prefix_columns (list): Colunas usadas para formar o prefixo.
        hash_func (str): Algoritmo de hash (default: 'sha256').
    
    Returns:
        pd.Series: Série contendo o hash de cada linha com prefixo.
    """
    hasher = getattr(hashlib, hash_func)
    
    def hash_row(row):
        prefix = '_'.join(str(row[col]) for col in prefix_columns)
        content = ''.join(str(row[col]) for col in hash_columns)
        full_hash = hasher(content.encode()).hexdigest()
        return f"{prefix}_{full_hash}"
    
    return df.apply(hash_row, axis=1)