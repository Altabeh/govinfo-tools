"""
Provides a script for downloading large amount of data
from www.govinfo.com and uploads a gzipped bulk file
to an `AWS S3 bucket`.
"""
import _sys
import logging

import boto3
import smart_open
from boto3.s3.transfer import S3Transfer
from botocore.exceptions import ClientError

__author__ = {"github.com/": ["altabeh"]}
__all__ = ['SS3']


class SS3(object):
    """
    Data transfer from and to S3 bucket.
    """

    def __init__(self, **kwargs):
        self.secret_key = kwargs.get('secret_key', None)
        self.public_key = kwargs.get('public_key', None)
        self.bucket_name = kwargs.get('bucket_name', None)
        self.client = boto3.client('s3', aws_access_key_id=self.public_key,
                                   aws_secret_access_key=self.secret_key,)
        for key, value in kwargs.items():
            if not value:
                raise Exception(f'`{key}` cannot be empty')

    def save(self, key, file_path, content=None, extra_args=None):
        """
        Save file under path `file_path` under `key`. If the object is not a file,
        `content` has to be not `None`.
        """
        if extra_args is None:
            extra_args = {}

        transfer = S3Transfer(self.client)
        bucket = self.bucket_name
        if content is None:
            transfer.upload_file(file_path, bucket, key, extra_args=extra_args)
        else:
            self.client.Object(
                bucket, key, ExtraArgs=extra_args,).put(Body=content)

        print(f'{key} has been created')

    def streamer(self, content, key_location, filename, ext):
        """
        Dump string `content` in a file with `filename` and extension `ext`
        inside `key_location`.
        """
        args = (self.public_key, self.secret_key,
                self.bucket_name, key_location, filename, ext)
        with smart_open.open('s3://%s:%s@%s/%s%s.%s' % args, 'w') as f:
            f.write(content)

    def delete(self, key):
        """
        Delete `key`.
        """
        self.client.Object(
            self.bucket_name, key).delete()
        print(f'{key} has been permanently deleted')

    def is_key(self, key_location, file_name, ext):
        """
        Check to see file `filename` with extension `ext`
        exists inside location `key_location` or not.
        Returns the length of the object.
        """
        bucket = boto3.resource('s3', aws_access_key_id=self.public_key,
                                aws_secret_access_key=self.secret_key,).Bucket(self.bucket_name)
        obj = list(bucket.objects.filter(Prefix='%s%s.%s' %
                                         (key_location, file_name, ext)))
        return len(obj)

    def create_presigned_url(self, object_name, expiration=3600):
        """
        Generate a presigned URL to share an S3 object.

        Args
        ----
        :param expiration: ---> int: time in seconds for the presigned URL to remain valid. 
        """
        try:
            response = self.client.generate_presigned_url('get_object',
                                                          Params={'Bucket': self.bucket_name, 'Key': object_name}, ExpiresIn=expiration)
        except ClientError as e:
            logging.error(e)
            return None
        return response


def main():
    """
    Example script for using `ginfo` crawler and `SS3` class.
    """
    from ginfo.ginfo import Ginfo
    from pathlib import Path
    collection = 'USCOURTS'
    initial_date = '2000-01-01'
    final_date = '2020-12-08'
    nature_suit = ['Patent']

    for n in nature_suit:
        g = Ginfo(collection=collection, nature_suit=n,
                  initial_date=initial_date, final_date=final_date, print_to_console=True)
        g.seal_results()
        g.collect_data_metadata()
        g.bulk_serialize()
        g.seal_bulk_data()
        gzipped_data = g.gzip_bulk_data()

        # Create an S3 bucket key to store gzipped_data.
        key = f'{collection}/{n}/{Path(gzipped_data).name}'
        s3 = SS3(secret_key='', public_key='', bucket_name='')
        s3.save(key, gzipped_data)


if __name__ == '__main__':
    main()
