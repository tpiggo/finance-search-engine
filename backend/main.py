import datetime
import numpy as np

from backend.s3 import S3Service

if __name__ == '__main__':
    print(datetime.datetime.now())
    df = S3Service().request_for_on_set('1INCHUSDT')
    print(datetime.datetime.now())
    df['open_time_ts'] = df['open_time'].values.astype(np.int64) // 10 ** 9
    final_df = df.loc[df['open_time_ts'].mod(86400).eq(0)]
    print("s3")
