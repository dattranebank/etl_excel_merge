import re
import numpy as np
import pandas as pd

def clean_columns(df_all):
    """
    Chuẩn hoá tên cột:
    - Bỏ khoảng trắng thừa 2 đầu
    - Thay \n bằng space
    - Gom nhiều khoảng trắng/tab/newline thành 1 space
    """
    df = df_all.rename(columns=lambda x: re.sub(r"\s+", " ", str(x)).strip())
    return df
