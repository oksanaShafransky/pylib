import numpy as np


def change_percentage(samples):
    current_sample = samples[-1:][0]
    history_samples = samples[:-1]
    history_average = np.mean(history_samples)
    return ((current_sample - history_average) / history_average) * 100


def distance_from_average(samples):
    current_sample = samples[-1:][0]
    history_samples = samples[:-1]
    return np.abs(
        (current_sample-np.mean(history_samples)) / np.std(history_samples)
    )


def calc_regression(samples, min_threshold=1.8):
    y = np.matrix(samples)
    x = np.matrix(range(0, len(samples)))
    yav = np.mean(y)
    xav = np.mean(x)
    y1 = y - yav
    x1 = x - xav
    beta1 = (y1 * np.transpose(x1)) / (x1 * np.transpose(x1))
    beta0 = yav - beta1 * xav
    delta = y - beta1 * x - beta0
    SRSS = np.sqrt(delta * np.transpose(delta) / float(len(samples) - 2))
    dx = np.sqrt(x1 * np.transpose(x1))
    res = np.abs(beta1) / (SRSS / dx) - min_threshold

    if res < 0:
        res = 0
    if res > 1.0:
        res = 1.0
    # if res > 0:
    #     slope_assumed = 1.0
    # else:
    #     slope_assumed = 0.0
    # return float(np.sign(beta1) * res), float(slope_assumed * beta1[0, 0])
    return float(res)
