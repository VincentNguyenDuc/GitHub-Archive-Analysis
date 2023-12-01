import pandas as pd
def rename_cols(df: pd.DataFrame, prefix: str) -> None:
    """Add a prefix to every column of a dataframe

    Args:
        df (pd.DataFrame): a dataframe
        prefix (str): a string
    """
    col_names = df.columns.to_list()
    new_col_names = list(map(lambda name: f"{prefix}_{name}", col_names))
    df.columns = new_col_names
