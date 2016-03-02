from boto import connect_s3
from collections import namedtuple
import commands

S3Bucket = namedtuple('S3Bucket', ['bucket_name', 'aws_access_key_id', 'aws_secret_access_key'])

class S3Backup(object):

    @staticmethod
    def backup_hdfs_path(s3_bucket, from_path, target_path):
        """
        backup the folder and update if it succeeded or failed
        """
        cmd = 'hadoop distcp -D fs.s3a.secret.key=\'%s\' -D\'mapreduce.job.name=retention backup to s3: %s\' -overwrite -pb -delete -strategy dynamic %s s3a://%s@%s/%s' % (
            s3_bucket.aws_secret_access_key, from_path, from_path, s3_bucket.aws_access_key_id, s3_bucket.bucket_name, target_path)

        state, output = commands.getstatusoutput(cmd)
        if state == 0:
            print('backup succeeded for path: %s' % (from_path))
            return True
        else:
            print('backup failed for path: %s. reason: %s' % (from_path, output))
            return False