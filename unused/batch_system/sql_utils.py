"""
SQL処理ユーティリティモジュール

SQLクエリの操作、期間条件の生成、テーブルエイリアス検出等
"""
import re
from datetime import datetime, timedelta, date
from typing import Dict, Tuple, List, Optional
from src.core.logging.logger import get_logger

logger = get_logger(__name__)


def extract_columns_mapping(sql_query: str) -> Dict[str, str]:
    """
    SQLクエリからカラムマッピングを抽出
    
    Args:
        sql_query (str): SQLクエリ
        
    Returns:
        Dict[str, str]: カラムマッピング辞書
    """
    columns_mapping = {}
    
    # AS句を使ったカラムマッピングの抽出
    pattern = r'(\w+\.\w+|\w+)\s+AS\s+"([^"]+)"'
    matches = re.findall(pattern, sql_query, re.IGNORECASE)
    
    for match in matches:
        original_column = match[0]
        alias_column = match[1]
        columns_mapping[alias_column] = original_column
        
    logger.debug(f"カラムマッピング抽出: {columns_mapping}")
    return columns_mapping


def find_table_alias(sql_query: str) -> str:
    """
    SQLクエリからメインテーブルのエイリアスを検出
    
    Args:
        sql_query (str): SQLクエリ
        
    Returns:
        str: テーブルエイリアス
    """
    # FROMの後にあるテーブル名とエイリアスを抽出
    pattern = r'FROM\s+(\w+)\s+(\w+)'
    match = re.search(pattern, sql_query, re.IGNORECASE)
    
    if match:
        table_alias = match.group(2)
        logger.debug(f"テーブルエイリアス検出: {table_alias}")
        return table_alias
    
    logger.warning("テーブルエイリアスが見つかりません")
    return ""


def find_submission_table_alias(sql_query: str) -> str:
    """
    提出関連テーブルのエイリアスを検出
    
    Args:
        sql_query (str): SQLクエリ
        
    Returns:
        str: 提出テーブルのエイリアス
    """
    # submission関連テーブルのパターンを検索
    patterns = [
        r'(\w+)\s*\.\s*submission_deadline',
        r'(\w+)\s*\.\s*submitted_at',
        r'(\w+)\s*\.\s*submission_status'
    ]
    
    for pattern in patterns:
        match = re.search(pattern, sql_query, re.IGNORECASE)
        if match:
            alias = match.group(1)
            logger.debug(f"提出テーブルエイリアス検出: {alias}")
            return alias
    
    logger.warning("提出テーブルエイリアスが見つかりません")
    return ""


def preprocess_sql_query(sql_query: str) -> str:
    """
    SQLクエリの前処理
    
    Args:
        sql_query (str): 元のSQLクエリ
        
    Returns:
        str: 前処理済みSQLクエリ
    """
    # 不要な空白を削除
    sql_query = re.sub(r'\s+', ' ', sql_query.strip())
    
    # セミコロンの処理
    if not sql_query.endswith(';'):
        sql_query += ';'
        
    logger.debug("SQLクエリ前処理完了")
    return sql_query


def detect_and_remove_group_by(sql_query: str) -> Tuple[str, bool]:
    """
    GROUP BY句の検出と削除
    
    Args:
        sql_query (str): SQLクエリ
        
    Returns:
        Tuple[str, bool]: (GROUP BY削除済みクエリ, GROUP BYが存在したか)
    """
    group_by_pattern = r'\bGROUP\s+BY\b.*?(?=\bORDER\s+BY\b|\bLIMIT\b|\bHAVING\b|;|$)'
    
    has_group_by = bool(re.search(group_by_pattern, sql_query, re.IGNORECASE))
    
    if has_group_by:
        sql_query = re.sub(group_by_pattern, '', sql_query, flags=re.IGNORECASE)
        logger.debug("GROUP BY句を削除しました")
    
    return sql_query, has_group_by


def detect_and_replace_subqueries(sql_query: str) -> Tuple[str, List[str]]:
    """
    サブクエリの検出と置換
    
    Args:
        sql_query (str): SQLクエリ
        
    Returns:
        Tuple[str, List[str]]: (サブクエリ置換済みクエリ, サブクエリリスト)
    """
    subqueries = []
    
    # サブクエリのパターン
    subquery_pattern = r'\([^()]*SELECT[^()]*\)'
    matches = re.findall(subquery_pattern, sql_query, re.IGNORECASE)
    
    for i, subquery in enumerate(matches):
        placeholder = f"__SUBQUERY_{i}__"
        subqueries.append(subquery)
        sql_query = sql_query.replace(subquery, placeholder, 1)
        
    if subqueries:
        logger.debug(f"サブクエリを{len(subqueries)}個検出・置換しました")
    
    return sql_query, subqueries


def restore_subqueries(sql_query: str, subqueries: List[str]) -> str:
    """
    サブクエリの復元
    
    Args:
        sql_query (str): SQLクエリ
        subqueries (List[str]): サブクエリリスト
        
    Returns:
        str: サブクエリ復元済みクエリ
    """
    for i, subquery in enumerate(subqueries):
        placeholder = f"__SUBQUERY_{i}__"
        sql_query = sql_query.replace(placeholder, subquery)
        
    logger.debug("サブクエリを復元しました")
    return sql_query


def check_and_prepare_where_clause(sql_query: str, additional_conditions: List[str]) -> str:
    """
    WHERE句の確認と条件追加の準備
    
    Args:
        sql_query (str): SQLクエリ
        additional_conditions (List[str]): 追加条件のリスト
        
    Returns:
        str: 条件追加済みSQLクエリ
    """
    if not additional_conditions:
        return sql_query
    
    # WHERE句の存在確認
    has_where = bool(re.search(r'\bWHERE\b', sql_query, re.IGNORECASE))
    
    # 条件を結合
    conditions_str = " AND ".join(additional_conditions)
    
    if has_where:
        # 既存のWHERE句に条件を追加
        where_pattern = r'(\bWHERE\b)'
        replacement = f'\\1 {conditions_str} AND'
        sql_query = re.sub(where_pattern, replacement, sql_query, flags=re.IGNORECASE)
    else:
        # 新しいWHERE句を追加
        # ORDER BY、GROUP BY、LIMIT等の前に挿入
        insert_pattern = r'(\s*(?:\bORDER\s+BY\b|\bGROUP\s+BY\b|\bLIMIT\b|;))'
        replacement = f' WHERE {conditions_str}\\1'
        
        if re.search(insert_pattern, sql_query, re.IGNORECASE):
            sql_query = re.sub(insert_pattern, replacement, sql_query, flags=re.IGNORECASE)
        else:
            # 末尾に追加
            sql_query = sql_query.rstrip(';') + f' WHERE {conditions_str};'
    
    logger.debug(f"WHERE句に条件を追加: {conditions_str}")
    return sql_query


def generate_period_condition(period_condition: str, column_name: str, table_alias: str) -> str:
    """
    期間条件を生成
    
    Args:
        period_condition (str): 期間条件文字列
        column_name (str): 日付カラム名
        table_alias (str): テーブルエイリアス
        
    Returns:
        str: 生成された期間条件SQL
    """
    logger.info(f"期間条件生成開始: period_condition='{period_condition}', column_name='{column_name}', table_alias='{table_alias}'")
    
    date_column = f"{table_alias}.{column_name}" if table_alias else column_name
    logger.info(f"期間条件生成中: date_column={date_column}, period_condition={period_condition}")
    
    try:
        today = datetime.now().date()
        yesterday = today - timedelta(days=1)
        
        if "前日まで累積" in period_condition:
            # 前日まで累積の範囲条件を生成
            if "～前日まで累積" in period_condition:
                start_date_str = period_condition.split("～前日まで累積")[0]
                start_date_str = start_date_str.replace("年", "-").replace("月", "-").replace("日", "")
                
                condition = f" DATE({date_column}) BETWEEN '{start_date_str}' AND '{yesterday}'"
                logger.info(f"前日まで累積範囲条件生成: {condition}")
                return condition
        
        elif "当日のみ" in period_condition:
            condition = f" DATE({date_column}) = '{today}'"
            logger.info(f"当日のみ条件生成: {condition}")
            return condition
            
        elif "前日のみ" in period_condition:
            condition = f" DATE({date_column}) = '{yesterday}'"
            logger.info(f"前日のみ条件生成: {condition}")
            return condition
            
        elif "過去7日間" in period_condition:
            week_ago = today - timedelta(days=7)
            condition = f" DATE({date_column}) BETWEEN '{week_ago}' AND '{today}'"
            logger.info(f"過去7日間条件生成: {condition}")
            return condition
            
        elif "過去30日間" in period_condition:
            month_ago = today - timedelta(days=30)
            condition = f" DATE({date_column}) BETWEEN '{month_ago}' AND '{today}'"
            logger.info(f"過去30日間条件生成: {condition}")
            return condition
        
        else:
            logger.warning(f"未対応の期間条件: {period_condition}")
            return ""
            
    except Exception as e:
        logger.error(f"期間条件生成エラー: {e}")
        return ""


def add_conditions_to_sql(sql_query: str, input_values: Dict, input_fields_types: Dict, 
                         deletion_exclusion: str, skip_deletion_exclusion: bool = False) -> str:
    """
    SQLクエリに条件を追加
    
    Args:
        sql_query (str): 元のSQLクエリ
        input_values (Dict): 入力値辞書
        input_fields_types (Dict): フィールドタイプ辞書  
        deletion_exclusion (str): 削除除外条件
        skip_deletion_exclusion (bool): 削除除外条件をスキップするか
        
    Returns:
        str: 条件追加済みSQLクエリ
    """
    conditions = []
    
    # 入力値に基づく条件追加
    for field_name, value in input_values.items():
        if value and str(value).strip():
            field_type = input_fields_types.get(field_name, 'text')
            
            if field_type == 'date':
                if isinstance(value, dict) and 'start_date' in value and 'end_date' in value:
                    start_date = value.get('start_date')
                    end_date = value.get('end_date')
                    if start_date and end_date:
                        conditions.append(f"DATE({field_name}) BETWEEN '{start_date}' AND '{end_date}'")
            else:
                # テキスト検索
                conditions.append(f"{field_name} LIKE '%{value}%'")
    
    # 削除除外条件の追加
    if not skip_deletion_exclusion and deletion_exclusion:
        conditions.append(deletion_exclusion)
        logger.debug("削除除外条件が追加されました")
    
    # 条件をSQLに追加
    if conditions:
        sql_query = check_and_prepare_where_clause(sql_query, conditions)
    
    logger.info(f"条件追加完了: {len(conditions)}個の条件を追加")
    return sql_query