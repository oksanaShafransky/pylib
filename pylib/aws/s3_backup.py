from boto import connect_s3
from collections import namedtuple
import subprocess

S3Bucket = namedtuple('S3Bucket', ['bucket_name', 'aws_access_key_id', 'aws_secret_access_key'])


class S3Backup(object):

    @staticmethod
    def backup_hdfs_path(s3_bucket, from_path, target_path):
        """
        backup the folder and update if it succeeded or failed
        """
        cmd = 'hadoop distcp -D fs.s3a.secret.key=\'%s\' -D\'mapreduce.job.name=retention backup to s3: %s\' -overwrite -pb -delete -strategy dynamic %s s3a://%s@%s/%s' % (
            s3_bucket.aws_secret_access_key, from_path, from_path, s3_bucket.aws_access_key_id, s3_bucket.bucket_name, target_path)

        try:
            _ = subprocess.check_output(cmd, shell=True)
            print('backup succeeded for path: %s' % from_path)
            return True
        except subprocess.CalledProcessError as cpe:
            print('backup failed for path: %s. reason: %s' % (from_path, cpe.output))
            return False
