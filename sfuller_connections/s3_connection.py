import pandas as pd
import s3fs
import time
import os

from .configs import S3Config


class S3ConnectCSV:
    def __init__(self, csv, config: S3Config, force_ints=False):
        self.csv = csv
        self.config = config
        self.force_ints = force_ints

    def s3_create(self):
        s3_bucket_name = self.config.s3_bucket
        filename = '/app/' + self.csv + '.csv'
        df = pd.read_csv(filename)
        if self.force_ints:
            for col in self.force_ints:
                df[col] = df[col].astype('Int64')
        timestr = time.strftime("%Y%m%d")
        filename = self.csv + '_output_' + timestr + '.csv'
        fs = s3fs.S3FileSystem(anon=False)

        with fs.open(s3_bucket_name + filename, 'w') as f:
            f.write(df.to_csv(None, index=False, header=False))

class S3Connect:
    def __init__(self, config: S3Config, bucket = os.getenv("S3_DEFAULT")):
        self.config = config
        self.bucket = bucket

    def s3_create(self, df, name, force_ints=False):
        s3_bucket_name = self.bucket

        if force_ints:
            for col in force_ints:
                df[col] = df[col].astype('Int64')

        filename = name + '.csv'
        fs = s3fs.S3FileSystem(anon=False, key=self.config.aws_key, secret=self.config.aws_secret)

        with fs.open(s3_bucket_name + filename, 'wb') as f:
            f.write(df.to_csv(None, index=False, header=False, encoding='utf-8', sep = ';').encode('utf-8'))

        return s3_bucket_name + filename
    
    def s3_append(self, df, name, force_ints=False):
        s3_bucket_name = self.bucket

        if force_ints:
            for col in force_ints:
                df[col] = df[col].astype('Int64')

        filename = name + '.csv'
        fs = s3fs.S3FileSystem(anon=False, key=self.config.aws_key, secret=self.config.aws_secret)

        with fs.open(s3_bucket_name + filename, 'ab') as f:
            f.write(df.to_csv(None, index=False, header=False, encoding='utf-8', sep = ';').encode('utf-8'))

        return s3_bucket_name + filename

    def s3_read(self, name, header):
        s3_bucket_name = self.bucket

        filename = name + '.csv'
        fs = s3fs.S3FileSystem(anon=False, key=self.config.aws_key, secret=self.config.aws_secret)
        
        with fs.open(s3_bucket_name + filename, 'rb', encoding="utf-8") as f:
            df = pd.read_csv(f, names=header, engine='python', 
                encoding="utf-8", 
                sep = ";"
            )

        return df
