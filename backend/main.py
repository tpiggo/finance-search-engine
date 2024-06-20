import datetime
import numpy as np

from backend.app.s3 import S3Service


def find_end_values(df):
    df['open_time_ts'] = df['open_time'].values.astype(np.int64) // 10 ** 9
    return df.loc[df['open_time_ts'].mod(86400).eq(0)]


if __name__ == '__main__':
    print(datetime.datetime.now())
    service = S3Service()
    out = service.get_all_sets()
    print(datetime.datetime.now())
    print("s3")
