from collections import Counter
import pandas as pd

def filter_by_column(df_A, df_B, column_name):
    filtered_results = []
    for idx, row in df_A.iterrows():
        value = row[column_name]
        if pd.isna(value):
            filtered_results.append(([], f"{column_name} が欠損値"))
        else:
            matches = df_B[df_B[column_name] == value]
            if matches.empty:
                filtered_results.append(([], f"{column_name} 対応なし"))
            else:
                filtered_results.append((matches, f"{column_name} 対応あり"))
    return filtered_results

def calculate_match_rate(name_A, names_B):
    match_rates = []
    counter_A = Counter(name_A)
    for name_B in names_B:
        counter_B = Counter(name_B)
        intersection = counter_A & counter_B
        match_rate = sum(intersection.values()) / len(name_A)
        match_rates.append((name_B, match_rate))
    return match_rates

def main_matching_process(df_A, df_B, search_col_A="検索用建物名", search_col_B="検索用物件名", filter_cols=None):
    if filter_cols is None:
        filter_cols = ["所在地", "築年月", "地上総階数", "総戸数"]

    results = []
    for idx, row in df_A.iterrows():
        candidates = df_B
        filter_results = {}

        for col in filter_cols:
            filter_result = filter_by_column(df_A.loc[[idx]], candidates, col)
            if filter_result and isinstance(filter_result[0][0], pd.DataFrame):
                candidates = filter_result[0][0]
            filter_results[col] = filter_result[0][1]

        match_rates = calculate_match_rate(row[search_col_A], candidates[search_col_B].tolist() if not candidates.empty else [])
        filter_count = sum([filter_results[col] == f"{col} 対応あり" for col in filter_cols])
        threshold = {4: 0.7, 3: 0.75, 2: 0.85, 1: 0.9, 0: 0.95}[filter_count]

        best_match = max(match_rates, key=lambda x: x[1]) if match_rates else (None, 0)
        results.append(best_match[0] if best_match[1] >= threshold else "閾値により除去")

    df_A["突合列"] = results
    return df_A

