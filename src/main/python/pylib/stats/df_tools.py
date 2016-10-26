
from pyspark.sql.functions import abs, min, isnan


def df_compare(df_left, df_right, join_columns, sort_by_column, threshold=0.01, cmp_type='abs', col_trans_left=None, col_trans_right=None):
    common_columns = set(df_left.columns).intersection(df_right.columns)
    merged_columns = list(join_columns)
    compared_columns = common_columns - set(merged_columns)

    left_joint, right_joint = df_left, df_right
    for com_col in compared_columns:
        left_joint = left_joint.withColumnRenamed(com_col, '%s_left' % com_col)
        right_joint = right_joint.withColumnRenamed(com_col, '%s_right' % com_col)

    joint = left_joint.join(right_joint, on=merged_columns)
    for cmp_col in compared_columns:
        joint = joint.withColumn('%s_diff' % cmp_col,
                                 abs(right_joint['%s_right' % cmp_col] - left_joint['%s_left' % cmp_col])
                                 if cmp_type.lower() in ['abs', 'absolute'] else
                                 abs(right_joint['%s_right' % cmp_col] - left_joint['%s_left' % cmp_col]) /
                                 min(right_joint['%s_right' % cmp_col], left_joint['%s_left' % cmp_col]))

    filtered = joint.filter(~ isnan(joint['%s_right' % sort_by_column]) & ~ isnan(joint['%s_left' % sort_by_column]))
    return filtered.orderBy('%s_diff' % sort_by_column, ascending=False)
