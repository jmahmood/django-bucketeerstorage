import base64
import os
import tempfile

from django.core.files import File
from django.core.files.storage import Storage

import boto3

AWS_ACCESS_KEY_ID = os.getenv('BUCKETEER_AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('BUCKETEER_AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('BUCKETEER_AWS_REGION')
AWS_BUCKET_NAME = os.getenv('BUCKETEER_BUCKET_NAME')


class BucketeerStorage(Storage):
    """
    Store your files on Bucketeer; no more problems with the ephemeral file system.
    """

    def upload(self, path):
        return self.client.upload_from_path(path)

    def __init__(self):
        super(BucketeerStorage, self).__init__()
        self.client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        )

        self.resource = boto3.resource('s3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

        self.bucket = self.resource.Bucket(AWS_BUCKET_NAME)

    def _open(self, name, mode='rb'):

        # download file into current directory

        tempf, temppath = tempfile.mkstemp('bucketeer')
        tempf.close()
        self.bucket.download_file(name, temppath)
        return File(temppath)

    def get_available_name(self, name, max_length=None):
        return name

    def _save(self, name, content):
        """
        Saves new content to the file specified by name. The content should be
        a proper File object or any python file-like object, ready to be read
        from the beginning.
        """
        self.client.upload_file(name, AWS_BUCKET_NAME, name)

        data = {
            'image': base64.b64encode(content.read()),
            'type': 'base64',
            'meta': {}
        }
        ret = self.client.make_request('POST', 'upload', data, True)
        content.close()
        return ret["id"]

    def url(self, name):
        return "https://{0}.s3.amazonaws.com/public/{1}".format(AWS_BUCKET_NAME, name)

    def listdir(self, path):
        """This is probably not a very good idea for large buckets."""
        files = [f.key.replace('{0}/'.format(path), '', 1) for f in self.bucket.objects.all() if f.key.startswith(path)]
        directories = {f.split('/', maxsplit=1)[0] for f in files if '/' in f}
        return list(directories), files

    def exists(self, name):
        """Probably a better way of doing this."""
        return name in self.listdir('/')

    def delete(self, name):
        self.client.Object(AWS_BUCKET_NAME, name).delete()
