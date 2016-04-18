__author__ = 'Felix'

import sys
import argparse
from datetime import datetime, timedelta
from snakebite.errors import FileNotFoundException
from hdfs_util import create_client, get_size


def date_suffix(dt):
    return dt.strftime('year=%y/month=%m/day=%d')


def send_report(report, date, recipients):
    import smtplib
    from email.mime.text import MIMEText

    smtp_srv = "mta01.sg.internal"
    sender = 'reports@similarweb.com'

    msg = MIMEText(report)
    msg['Subject'] = 'Daily aggregation size change report %s' % date
    msg['From'] = sender
    msg['To'] = recipients

    send = smtplib.SMTP(smtp_srv)
    send.sendmail(sender, recipients.split(','), str(msg))
    send.quit()


def list_collectors(base_dir, date=None, output_prefix='aggkey'):
    cl = create_client()
    ret = set()
    for entry in [d['path'] for d in cl.ls([base_dir], recurse=False)]:
        for field in entry.split('/'):
            if field.startswith('%s=' % output_prefix):
                if date is None:
                    ret.add(field[len(output_prefix) + 1:])
                else:
                    try:
                        cl.test('%s/%s' % (entry, date_suffix(date)))
                        ret.add(field[len(output_prefix) + 1:])
                    except FileNotFoundException:
                        pass

    return ret


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('agg_dir', metavar='TABLE', help='Name of HBase table 1, can be cluster.table_name (default is mrp)')
    parser.add_argument('check_date', metavar='TABLE', help='Name of HBase table 2, can be cluster.table_name')
    parser.add_argument('-m', '--mailing-list', dest='mail_to', required=False, default=None, help='mail report recipients, mail not sent in case this arg is not passed')
    parser.add_argument('-t', '--threshold', dest='threshold', required=False, default=0.05, help='change threshold to report')

    args = parser.parse_args()

    daily_agg_dir = args.agg_dir
    date = datetime.strptime(args.check_date, '%Y-%m-%d')
    threshold = args.threshold
    report = ''
    for col in list_collectors(daily_agg_dir, date=date):
        curr_size = get_size('%s/%s/%s' % (daily_agg_dir, 'aggkey=%s' % col, date_suffix(date)))
        previous_size = get_size('%s/%s/%s' % (daily_agg_dir, 'aggkey=%s' % col, date_suffix(date + timedelta(days=-1))))
        change = (curr_size - previous_size) / float(previous_size)

        last_week_size = get_size('%s/%s/%s' % (daily_agg_dir, 'aggkey=%s' % col, date_suffix(date + timedelta(days=-7))))
        last_week_previous_size = get_size('%s/%s/%s' % (daily_agg_dir, 'aggkey=%s' % col, date_suffix(date + timedelta(days=-8))))
        last_week_change = (last_week_size - last_week_previous_size) / float(last_week_previous_size)

        slope = (change - last_week_change) / last_week_change

        if change > threshold:
            report += '%s changed %.3f from yesterday, as opposed to %.3f daily a week ago, representing a %.2f slope\n' % (col, change, last_week_change, slope)

    print report
    if args.mail_to is not None:
        send_report(report, args.check_date, args.mail_to)
