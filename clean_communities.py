import pandas as pd


# Helper functions
def change_datatype(df, col, type):
    df[col] = df[col].astype(type)

def change_community_name(df, old_name, new_name):
    df.loc[df[df.Name == old_name].index, "Name"] = new_name


# Main function
def clean_communities(df1, df2):
    """
    Drops columns with missing information
    Changes datatypes according to the data model

    Input:
    df1 - Pandas dataframe
    df2 - Pandas dataframe

    Output:
    df_clean - cleaned Pandas dataframe
    """

    # Clean df1 
    df1.drop(df1[df1["No"] == "Total"].index[0], inplace=True)
    
    for col in ["Population", "Densitysqmi2", "Densitykm2"]:
        df1[col] = [float(str(i).replace(",", "")) for i in df1[col]]

    change_datatype(df1, "No", int)
    change_datatype(df1, "Densitysqmi2", float)
    change_datatype(df1, "Densitykm2", float)

    change_community_name(df1, "(The) Loop[11]", "Loop")
    change_community_name(df1, "McKinley Park", "Mckinley Park")
    change_community_name(df1,"O'Hare", "Ohare")

    df1 = df1[["No", "Name", "Population", "Areakm", "Densitykm2"]] \
        .rename(columns={"No": "id", "Name": "name", "Population": "population", "Areakm": "area", "Densitykm2": "density"})

    # Clean df2
    df2.drop(df2[df2["Geography"] == "Chicago Overall"].index, inplace=True)

    df2["Estimate"] = [float(str(i).replace(",", ".")) for i in df2["Estimate"]]
    
    change_datatype(df2, "Estimate", float)

    df2 = df2[(df2.Year == 2020) & ((df2.Grouping == "Race") | (df2.Grouping == "Poverty"))][["Geography", "Grouping", "Indicator", "Estimate"]]
    
    df2 = df2.pivot(index="Geography", columns="Indicator", values="Estimate").reset_index()
    df2.drop(["Child poverty rate, 2020"], axis=1, inplace=True)
    df2 = df2.rename(columns={"Geography": "name", "% Asian, 2020": "asian_perc", "% Black, 2020": "black_perc", \
        "% White, 2020": "white_perc", "% Latino, 2020": "latino_perc", "% in poverty, 2020": "poverty_perc", \
        "% in extreme poverty, 2020": "ext_poverty_perc"})
    
    # Merge both dataframes 
    df_clean = df1.merge(df2, on="name", how="outer")

    return df_clean