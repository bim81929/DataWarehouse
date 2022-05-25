import pandas as pd
from hdfs.ext.kerberos import KerberosClient

from config import Config


class HDFSPublisher:

    def write(self, df: pd.DataFrame, filename: str, path: str) -> object:
        my_client = KerberosClient(f"{Config.HDFS_HOST}:{Config.HDFS_PORT}")
        with my_client.write(path + filename, encoding='utf-8') as writer:
            df.to_csv(writer)
