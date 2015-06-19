__author__ = 'Felix'

from stats import check_output


def find_files(path):
    cmd = ['hadoop', 'fs', '-ls', path]
    ls_out = check_output()