import pandas as pd


def clean_weather(df):
    """
    Drops columns with missing information
    Changes datatypes according to the data model

    Input:
    df - Pandas dataframe

    Output:
    df_clean - cleaned Pandas dataframe
    """

    df.drop(["snow", "wpgt", "tsun"], axis=1, inplace=True)
    df.time = pd.to_datetime(df.time)
    df.coco = df.coco.astype(int)

    df_clean = df[["time", "temp", "prcp", "wspd", "coco"]] \
        .rename(columns={"time": "hour", "prcp": "rain", "wspd": "wind_speed", "coco": "condition_code"})

    return df_clean
