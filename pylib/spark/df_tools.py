
from pyspark.sql.functions import udf, abs, min, isnan, isnull, coalesce, least, greatest
from pyspark.sql.types import BooleanType


def any_none(*vals):
    return any([val is None for val in vals])
any_null = udf(lambda *cols: any_none(*cols), BooleanType())


def all_none(*vals):
    return all([val is None for val in vals])
all_null = udf(lambda *cols: all_none(*cols), BooleanType())


def preserve_nulls(df, how='any', subset=None):
    if how == 'all':
        return df.filter(all_null(*(subset or df.columns)))
    elif how == 'any':
        return df.filter(any_null(*(subset or df.columns)))
    else:
        raise Exception('unknown method. choose between any and all')


def df_compare(df_left, df_right, join_columns, sort_by_column=None, compared_columns=None, threshold=0.01, cmp_type='abs',
               col_trans_left=None, col_trans_right=None):
    """
    compares two data frames to find most significant differences between them
    :param df_left: dataframe 1
    :param df_right: dataframe 2
    :param join_columns: mutual columns to join on
    :param sort_by_column: column name for output order (default is first column)
    :param compared_columns: list of column names to compare (default: all shared columns not in 'join_columns'
    :param threshold: significance threshold, only difference exceeding which will be returned.
           always calculated in relative terms
    :param cmp_type: abs(olute) or rel(ative) to calculate the change metrics, does not affect significance threshold
    :param col_trans_left: dictionary of column name translations for dataframe 1
    :param col_trans_right: dictionary of column name translations for dataframe 2
    :return: records with changes exceeding threshold, ordered by change of :sort_by_column
    """

    merged_columns = list(join_columns)

    if compared_columns is None:
        common_columns = set(df_left.columns).intersection(df_right.columns)
        compared_columns = common_columns - set(merged_columns)

    if sort_by_column is None:
        sort_by_column = list(compared_columns)[0]

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
        .filter(greatest(*[joint['%s_diff' % col] for col in compared_columns]) > threshold) \
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
        left_2_join = left_2_join.withColumn('%s_ind' % col.replace('.', '_'), left_2_join[col])
        right_2_join = right_2_join.withColumn('%s_ind' % col.replace('.', '_'), right_2_join[col])

    left_joint = left_joint.join(right_2_join, on=list(join_columns), how='left_outer')
    right_missing = preserve_nulls(left_joint, subset=['%s_ind' % col.replace('.', '_') for col in join_columns])

    right_joint = right_joint.join(left_2_join, on=list(join_columns), how='left_outer')
    left_missing = preserve_nulls(right_joint, subset=['%s_ind' % col.replace('.', '_') for col in join_columns])

    if sort_by_column is None:
        return right_missing, left_missing
    else:
        left_ret = right_missing.filter(~ isnan(df_left[sort_by_column])).orderBy(df_left[sort_by_column], ascending=False)
        right_ret = left_missing.filter(~ isnan(df_left[sort_by_column])).orderBy(df_left[sort_by_column], ascending=False)
        return left_ret, right_ret


def df_equal(df1, df2):
    """
    makes sure that all rows in df1 are in df2 (and vice versa)
    :param df_left: dataframe 1
    :param df_right: dataframe 2
    :return: (are dataframes equal)
    """
    return (df1.subtract(df2).count() == 0) and (df2.subtract(df1).count() == 0)


def df_equivalent(df_left, df_right, join_columns, sort_by_column, threshold=0.01, cmp_type='abs',
                  col_trans_left=None, col_trans_right=None):

    diff_r, diff_l = df_diff(df_left, df_right, join_columns, sort_by_column, col_trans_left, col_trans_right)
    comp = df_compare(df_left, df_right, join_columns, sort_by_column, threshold, cmp_type, col_trans_left, col_trans_right)

    ret = True

    if diff_r.count() != 0:
        print "right dataframe has unique keys. example:"
        print diff_r.first()
        ret = False
    if diff_l.count() != 0:
        print "left dataframe has unique keys. example:"
        print diff_l.first()
        ret = False
    if comp.count() != 0:
        print "some rows are not equivalent (threshold=%d). example:" % threshold
        print comp.first()
        ret = False

    return ret