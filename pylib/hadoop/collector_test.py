__author__ = 'Felix'

from datetime import timedelta
from snakebite.errors import FileNotFoundException

from hdfs_util import create_client, get_size


def __date_suffix(dt):
    return dt.strftime('year=%y/month=%m/day=%d')


def __send_report(report, date, recipients):
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


def __list_collectors(base_dir, date=None, output_prefix='aggkey'):
    cl = create_client()
    ret = set()
    for entry in [d['path'] for d in cl.ls([base_dir], recurse=False)]:
        for field in entry.split('/'):
            if field.startswith('%s=' % output_prefix):
                if date is None:
                    ret.add(field[len(output_prefix) + 1:])
                else:
                    try:
                        cl.test('%s/%s' % (entry, __date_suffix(date)))
                        ret.add(field[len(output_prefix) + 1:])
                    except FileNotFoundException:
                        pass

    return ret


def compare_collectors(daily_agg_dir, date, mailing_list=None, threshold=0.05):
    report = ''
    for col in __list_collectors(daily_agg_dir, date=date):
        try:
            print "Inspecting collector %s" % col
            curr_size = get_size('%s/%s/%s' % (daily_agg_dir, 'aggkey=%s' % col, __date_suffix(date)))
            previous_size = get_size(
                '%s/%s/%s' % (daily_agg_dir, 'aggkey=%s' % col, __date_suffix(date + timedelta(days=-1))))
            change = (curr_size - previous_size) / float(previous_size)

            last_week_size = get_size(
                '%s/%s/%s' % (daily_agg_dir, 'aggkey=%s' % col, __date_suffix(date + timedelta(days=-7))))
            last_week_previous_size = get_size(
                '%s/%s/%s' % (daily_agg_dir, 'aggkey=%s' % col, __date_suffix(date + timedelta(days=-8))))
            last_week_change = (last_week_size - last_week_previous_size) / float(last_week_previous_size)

            slope = (change - last_week_change) / last_week_change

            if change > threshold:
                report += '%s changed %.3f from yesterday, as opposed to %.3f daily a week ago, representing a %.2f ratio\n' % (
                    col, change, last_week_change, slope)
            else:
                print "No significant changes found for %s" % col
        except FileNotFoundException:
            print "Last week's data not found, skipping test for %s" % col
        except Exception, e:
            print "Error while inspecting %s: %s" % (col, str(e))

    # check if report is in fact empty
    if report != '':
        print(report)
        if mailing_list is not None:
            __send_report(report, date, mailing_list)
    else:
        print('No significant changes detected ')
