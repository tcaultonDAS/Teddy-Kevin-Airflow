import os
import errno

import boto3

import logging
logger = logging.getLogger(__name__)

class EmptyFileException(Exception):
    pass

class S3Client:

    def __init__(self, bucket, access_key_id=None, secret_access_key=None):
        """Create an s3 client with the supplied credentials.

        Parameters:
            bucket: the bucket name

        Other Parameters:
            access_key_id: the access key id
            secret_access_key: the secret access key
        """

        self.s3 = boto3.client('s3',
            aws_access_key_id = access_key_id,
            aws_secret_access_key = secret_access_key
        )
        self.bucket = bucket

    def upload_file(self, source, destination, purge=True):
        """Upload a file to S3.

        Parameters:
            source: the file to upload
            destination: the s3 path

        Other parameters:
            purge: delete the source file after upload
        """
        filename = os.path.basename(source)
        location = os.path.join(destination, filename).strip('/')
        logging.info(f'uploading {source} to s3://{self.bucket}/{location}')
        try:
            self.s3.upload_file(source, self.bucket, location)
        finally:
            if purge:
                os.remove(source)
                logging.info(f'Deleted file {source}.')

    def download_file(self, key, output):
        logging.info(f'Downloading\n\ts3://{self.bucket}/{key}\n\tto\n\t{output} ...')
        # -- if output folder does not exists create it
        # try:
        #     os.makedirs(os.path.dirname(output))
        # except OSError as exc:
        #     if exc.errno != errno.EEXIST:
        #         raise
        with open(output, 'wb') as out:
            self.s3.download_fileobj(self.bucket, key, out)
            logging.info("Download succesful")
            logging.info(out)

    def search(self, path):
        """Find objects containing this path.

        Parameters
            path: the substring to search for

        Returns
            a list of matching keys
        """
        objects = self.s3.list_objects(Bucket=self.bucket, Prefix=path)
        return [ obj['Key'] for obj in objects['Contents'] ] if 'Contents' in objects else [ ]

    def _is_file_size_less_than_500_bytes(self, name):
        if os.stat(name).st_size > 500:
            return False
        return True
