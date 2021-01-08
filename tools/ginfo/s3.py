"""
Provides a useful class for data transfer to and from
any S3 bucket.
"""
import asyncio
import concurrent.futures
import logging
from math import ceil
from os import cpu_count
from pathlib import Path

import boto3
import requests
import smart_open
from boto3.s3.transfer import S3Transfer
from botocore.exceptions import ClientError
from tqdm import tqdm

__author__ = {"github.com/": ["altabeh"]}
__all__ = ["SS3"]


class SS3(object):
    """
    Data transfer from and to S3 bucket.
    """

    def __init__(self, **kwargs):
        self.secret_key = kwargs.get("secret_key", None)
        self.public_key = kwargs.get("public_key", None)
        self.bucket_name = kwargs.get("bucket_name", None)
        self.client = boto3.client(
            "s3",
            aws_access_key_id=self.public_key,
            aws_secret_access_key=self.secret_key,
        )
        self.resource = boto3.resource(
            "s3",
            aws_access_key_id=self.public_key,
            aws_secret_access_key=self.secret_key,
        )
        for key, value in kwargs.items():
            if not value:
                raise Exception(f"`{key}` cannot be empty")
        # Expire S3 presigned urls after `presigned_expiration` seconds.
        self.presigned_expiration = kwargs.get("presigned_expiration", 3600)

    def fetch(self, directory, key, version_id=None, chunk_size=10000000):
        """
        Save S3 file with the `version_id` (str) and `key` (str) in a local `directory`.

        Args
        ----
        :param chunk_size: int: size of the chunks with which data transfer happens.
        """

        directory = Path(directory)
        file_path = directory / key
        file_path.parent.mkdir(parents=True, exist_ok=True)
        if not version_id:
            version_id = "null"
        obj = self.client.get_object(
            Bucket=self.bucket_name,
            Key=key,
            VersionId=version_id,
        )
        with open(file_path, "wb") as f:
            chunks = obj["Body"].iter_chunks(chunk_size=10000000)
            file_size = (
                self.resource.Bucket(self.bucket_name).Object(key).content_length
            )
            for chunk in tqdm(chunks, total=ceil(file_size / chunk_size)):
                f.write(chunk)

    def save(self, key, file_path, content=None, extra_args=None):
        """
        Save file under path `file_path` (str) under `key` (str). If the object is not a file,
        `content` (str) has to be not `None`.
        """
        if extra_args is None:
            extra_args = {}

        transfer = S3Transfer(self.client)
        bucket = self.bucket_name
        if content is None:
            transfer.upload_file(file_path, bucket, key, extra_args=extra_args)
        else:
            self.client.Object(
                bucket,
                key,
                ExtraArgs=extra_args,
            ).put(Body=content)

        print(f"{key} has been created")

    def streamer(self, content, key_location, filename, ext):
        """
        Dump string `content` in a file with `filename` (str) and extension `ext` (str)
        inside `key_location` (str).
        """
        args = (
            self.public_key,
            self.secret_key,
            self.bucket_name,
            key_location,
            filename,
            ext,
        )
        with smart_open.open("s3://%s:%s@%s/%s%s.%s" % args, "w") as f:
            f.write(content)

    def delete(self, key):
        """
        Delete `key`.
        """
        self.client.Object(self.bucket_name, key).delete()
        print(f"{key} has been permanently deleted")

    def is_key(self, key_location, file_name, ext):
        """
        Check to see file `filename` with extension `ext`
        exists inside location `key_location` or not.
        Returns the length of the object.
        """
        bucket = self.resource.Bucket(self.bucket_name)
        obj = list(
            bucket.objects.filter(Prefix="%s%s.%s" % (key_location, file_name, ext))
        )
        return len(obj)

    def create_presigned_url(self, key):
        """
        Generate a presigned url to share an S3 object `key` (str).
        """
        try:
            response = self.client.generate_presigned_url(
                "get_object",
                Params={"Bucket": self.bucket_name, "Key": key},
                ExpiresIn=self.presigned_expiration,
            )
        except ClientError as e:
            logging.error(e)
            return None
        return response

    async def get_size(self, key):
        """
        Get size of the S3 object `key`.
        """
        size = self.resource.Bucket(self.bucket_name).Object(key).content_length
        return size

    @staticmethod
    def download_range(url, start, end, output):
        """
        Iterate over response data and write to disk.

        Args
        ----
        :param start: ---> int: start-byte of the iteration.
        :param end: ---> int: end-byte of the iteration.
        :param output: ---> str: path to the output file.
        """
        headers = {"Range": f"bytes={start}-{end}"}
        response = requests.get(url, headers=headers)
        with open(output, "wb") as f:
            for part in response.iter_content(1024):
                f.write(part)

    async def async_download(self, executor, key, directory, chunk_size, url=None):
        """
        Uses asyncio to carry out streaming data from S3 `key` (str) and writing it to
        local `directory` (str) in chunks of size `chunk_size` (int) using a ThreadPool
        `executor` that accepts tasks defined by the `download_range` method.
        """
        loop = asyncio.get_event_loop()
        file_size = await self.get_size(key)
        chunks = range(0, file_size, chunk_size)
        if not url:
            url = self.create_presigned_url(key)
        directory = Path(directory)
        file_path = str(directory / key)
        tasks = [
            loop.run_in_executor(
                executor,
                self.download_range,
                url,
                start,
                start + chunk_size - 1,
                f"{file_path}.part{i}",
            )
            for i, start in enumerate(chunks)
        ]

        await asyncio.wait(tasks)
        with open(file_path, "wb") as f:
            chunks_length = len(chunks)
            for chunk in tqdm(range(chunks_length), total=chunks_length):
                chunk_path = f"{file_path}.part{chunk}"
                with open(chunk_path, "rb") as g:
                    f.write(g.read())
                Path(chunk_path).unlink()

    def execute_download(self, key, directory, chunk_size=10000000, url=None):
        """
        Start the async downloading of the S3 object `key` (str) to the local
        `directory` (str) in chunks of size `chunk_size` (int).

        Args
        ----
        :param url: ---> str: if bucket policy allows public access to object `key`,
                              directly enter its get `url`.
        """
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=cpu_count())
        loop = asyncio.get_event_loop()
        try:
            loop.run_until_complete(
                self.async_download(executor, key, directory, chunk_size, url)
            )
        finally:
            loop.close()