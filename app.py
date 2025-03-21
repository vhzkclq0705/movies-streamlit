import os
import altair as alt
import matplotlib.pyplot as plt
import pandas as pd
import pyarrow.dataset as ds
import streamlit as st

# Functions
def convert_float_to_int(n):
    try:
        n = float(n)
        return int(n) if n.is_integer() else round(n, 1)
    except Exception:
        return n

# @st.cache_data
def load_data() -> pd.DataFrame:
    """
    영화 데이터를 불러온 후, 날짜 포맷을 변경해주고 데이터 프레임으로 반환하는 함수
    
    :return df: 영화 정보 데이터프레임
    """
    num_cols = [
        'salesAmt', 'scrnCnt', 'showCnt', 'salesInten', 'salesChange',
        'audiInten', 'audiChange', 'audiCnt', 'audiAcc', "rank", "rnum"
    ]
    path = os.path.expanduser("~/swcamp4/data/movies_after/meta/meta")
    dataset = ds.dataset(path, format="parquet", partitioning="hive")
    
    df = dataset.to_table().to_pandas()
    df["dt"] = pd.to_datetime(df["dt"], format="%Y%m%d", errors="coerce")
    df = df.applymap(convert_float_to_int)
    df = df.sort_values(by="audiAcc", ascending=False).reset_index(drop=True)
    
    return df

# Properties
df = load_data()
base_cols = ["movieNm", "audiAcc", "multiMovieYn", "repNationCd", "dt"]
options = list(set(df.columns) - set(base_cols))
default_cols = [col for col in base_cols if col in options]

# UI
st.title("2024 KOBIS Daily Box Office Data Analytics")

selected_cols = st.multiselect(
    "Data Preview",
    options=options,
    default=default_cols,
)
st.dataframe(df[base_cols + selected_cols].head(5))

st.markdown(
    f"""
    ## NaN ratio of multiMovieYn and repNationCd
    multiMovieYn NaN: {df["multiMovieYn"].isna().mean() * 100:.2f}%\n
    repNationCd NaN: {df["repNationCd"].isna().mean() * 100:.2f}%
    """            
)
