from collections import Counter
import pandas as pd

def filter_by_column(df_source, df_target, column_name):
    """
    突合元 (df_source) の各行について、突合先 (df_target) の column_name と一致するデータを取得する。
    """
    filtered_results = []
    for idx, row in df_source.iterrows():
        value = row[column_name]
        if pd.isna(value):
            filtered_results.append(([], f"{column_name} が欠損値"))
        else:
            matches = df_target[df_target[column_name] == value]
            filtered_results.append((matches, f"{column_name} 対応あり" if not matches.empty else f"{column_name} 対応なし"))
    return filtered_results

def calculate_match_rate(source_str, target_list):
    """
    突合元の文字列と突合先のリストにある各文字列との類似度を計算する。
    """
    match_rates = []
    counter_source = Counter(source_str)
    for target_str in target_list:
        counter_target = Counter(target_str)
        intersection = counter_source & counter_target
        match_rate = sum(intersection.values()) / len(source_str) if source_str else 0
        match_rates.append((target_str, match_rate))
    return match_rates

def main_matching_process(df_source, df_target, source_match_col, target_match_col, filter_cols=None, match_rate_thresholds=None):
    """
    突合処理を実行し、結果を df_source に追加する。

    Args:
        df_source (pd.DataFrame): 突合元のデータフレーム
        df_target (pd.DataFrame): 突合先のデータフレーム
        source_match_col (str): 突合元の比較対象のカラム名
        target_match_col (str): 突合先の比較対象のカラム名
        filter_cols (list): フィルタリングに使用するカラム名リスト
        match_rate_thresholds (dict): フィルタ条件の一致数ごとの閾値
    """
    if filter_cols is None:
        filter_cols = []

    if match_rate_thresholds is None:
        match_rate_thresholds = {4: 0.7, 3: 0.75, 2: 0.85, 1: 0.9, 0: 0.95}

    results = []
    for idx, row in df_source.iterrows():
        candidates = df_target
        filter_results = {}

        for col in filter_cols:
            filter_result = filter_by_column(df_source.loc[[idx]], candidates, col)
            if filter_result and isinstance(filter_result[0][0], pd.DataFrame):
                candidates = filter_result[0][0]
            filter_results[col] = filter_result[0][1]

        match_rates = calculate_match_rate(row[source_match_col], candidates[target_match_col].tolist() if not candidates.empty else [])
        filter_count = sum([filter_results[col] == f"{col} 対応あり" for col in filter_cols])
        threshold = match_rate_thresholds.get(filter_count, 0.95)

        best_match = max(match_rates, key=lambda x: x[1]) if match_rates else (None, 0)
        results.append(best_match[0] if best_match[1] >= threshold else "閾値により除去")

    df_source["突合列"] = results
    return df_source

