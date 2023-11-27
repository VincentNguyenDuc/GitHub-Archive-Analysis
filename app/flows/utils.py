def rename_cols(df, df_name) -> None:
    col_names = df.columns.to_list()
    new_col_names = list(map(lambda name: f"{df_name}.{name}", col_names))
    df.columns = new_col_names
