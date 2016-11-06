__author__ = 'Felix'

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
