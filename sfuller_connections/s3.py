import os
from typing import NamedTuple
from boto3 import Session


# session = Session()
# credentials = session.get_credentials()
# current_credentials = credentials.get_frozen_credentials()


class S3Config(NamedTuple):
    aws_key: str
    aws_secret: str
    # s3_bucket: str

S3ConfigFromEnv = S3Config(aws_key=os.getenv("S3_KEY"),
                           aws_secret=os.getenv("S3_SECRET"),
                        #    s3_bucket=os.getenv("S3_BUCKET")
                           )


