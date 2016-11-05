
from pyspark.sql.functions import abs, min, isnan, isnull, coalesce, least


def df_compare(df_left, df_right, join_columns, sort_by_column, threshold=0.01, cmp_type='abs',
               col_trans_left=None, col_trans_right=None):
    """
    compares two data frames to find most significant differences between them
    :param df_left: dataframe 1
    :param df_right: dataframe 2
    :param join_columns: mutual columns to join on
    :param sort_by_column: compared metric column for significance ordering
    :param threshold: significance threshold, only difference exceeding which will be returned.
           always calculated in relative terms
    :param cmp_type: abs(olute) or rel(ative) to calculate the change metrics, does not affect significance threshold
    :param col_trans_left: dictionary of column name translations for dataframe 1
    :param col_trans_right: dictionary of column name translations for dataframe 2
    :return: records with changes exceeding threshold, ordered by change of :sort_by_column
    """

    common_columns = set(df_left.columns).intersection(df_right.columns)
    merged_columns = list(join_columns)
    compared_columns = common_columns - set(merged_columns)

    left_joint, right_joint = df_left, df_right

    if col_trans_left is not None:
        for old_name, rename in col_trans_left.items():
            left_joint = left_joint.withColumnRenamed(old_name, rename)

    if col_trans_right is not None:
        for old_name, rename in col_trans_right.items():
            right_joint = right_joint.withColumnRenamed(old_name, rename)

    for com_col in compared_columns:
        left_joint = left_joint.withColumnRenamed(com_col, '%s_left' % com_col)
        right_joint = right_joint.withColumnRenamed(com_col, '%s_right' % com_col)

    joint = left_joint.join(right_joint, on=merged_columns)
    for cmp_col in compared_columns:
        joint = joint.withColumn('%s_diff' % cmp_col,
                                 abs(right_joint['%s_right' % cmp_col] - left_joint['%s_left' % cmp_col])
                                 if cmp_type.lower() in ['abs', 'absolute'] else
                                 abs(right_joint['%s_right' % cmp_col] - left_joint['%s_left' % cmp_col]) /
                                 least(right_joint['%s_right' % cmp_col], left_joint['%s_left' % cmp_col]))

    return joint \
        .filter(~ isnan(joint['%s_right' % sort_by_column]) & ~ isnan(joint['%s_left' % sort_by_column])) \
        .filter((abs(joint['%s_right' % sort_by_column] - joint['%s_left' % sort_by_column]) /
                least(joint['%s_right' % sort_by_column], joint['%s_left' % sort_by_column])) > threshold) \
        .orderBy('%s_diff' % sort_by_column, ascending=False)


def df_diff(df_left, df_right, join_columns, sort_by_column=None, col_trans_left=None, col_trans_right=None):
    """
    compares two data frames to find records present in only one of them
    :param df_left: dataframe 1
    :param df_right: dataframe 2
    :param join_columns: mutual columns to join on
    :param sort_by_column: (optional) compared metric column for significance ordering. if omitted, order is random
    :param col_trans_left: dictionary of column name translations for dataframe 1
    :param col_trans_right: dictionary of column name translations for dataframe 2
    :return: (records missing from dataframe 2, records missing from dataframe 1)
    """

    left_joint, right_joint = df_left, df_right
    left_2_join, right_2_join = df_left, df_right

    if col_trans_left is not None:
        for old_name, rename in col_trans_left.items():
            left_joint = left_joint.withColumnRenamed(old_name, rename)
            left_2_join = left_2_join.withColumnRenamed(old_name, rename)

    if col_trans_right is not None:
        for old_name, rename in col_trans_right.items():
            right_joint = right_joint.withColumnRenamed(old_name, rename)
            right_2_join = right_2_join.withColumnRenamed(old_name, rename)

    for col in join_columns:
        left_2_join = left_2_join.withColumn('%s_ind' % col, left_2_join[col])
        right_2_join = right_2_join.withColumn('%s_ind' % col, right_2_join[col])

    left_joint = left_joint.join(right_2_join, on=list(join_columns), how='left_outer')
    right_missing = left_joint \
        .withColumn('join_indicator', coalesce(*['%s_ind' % col for col in join_columns])) \
        .filter(isnull('join_indicator'))

    right_joint = right_joint.join(left_2_join, on=list(join_columns), how='left_outer')
    left_missing = right_joint \
        .withColumn('join_indicator', coalesce(*['%s_ind' % col for col in join_columns])) \
        .filter(isnull('join_indicator'))

    if sort_by_column is None:
        return right_missing, left_missing
    else:
        left_ret = right_missing.filter(~ isnan(df_left[sort_by_column])).orderBy(df_left[sort_by_column], ascending=False)
        right_ret = left_missing.filter(~ isnan(df_left[sort_by_column])).orderBy(df_left[sort_by_column], ascending=False)
        return left_ret, right_ret
