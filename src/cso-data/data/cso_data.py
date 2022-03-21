# %%
import requests
import pandas as pd


pd.options.display.max_columns = 100
pd.options.display.max_rows = 100


def get_toc(frequency: bool = True, variables: bool = False) -> pd.DataFrame:
    url = "https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadCollection"
    try:
        json_data = requests.get(url).json()
    except requests.exceptions.SSLError:  # needed if behind firewall
        json_data = requests.get(url, verify=False).json()

    df = pd.json_normalize(json_data["link"]["item"], max_level=0)

    toc_df = (
        df["href"]
        .str.lstrip(
            "https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadDataset/"
        )
        .str.rstrip("/JSON-stat/2.0/en")
        .rename("table_id")
        .to_frame()
        .assign(table_name=df["label"], last_updated=pd.to_datetime(df["updated"]))
    )

    if frequency:
        dimension_df = pd.json_normalize(df["dimension"], max_level=0)
        tlist = pd.json_normalize(
            dimension_df[[c for c in dimension_df.columns if "TLIST" in c]].stack(),
            max_level=1,
        )
        t_labels = (
            tlist["category.label"]
            .apply(pd.Series)
            .stack()
            .rename_axis(index=["id", "key"])
            .rename("label")
            .reset_index()
            .groupby("id")
            .agg({"label": ["min", "max"]})
            .droplevel(0, axis="columns")
            .rename({"min": "earliest", "max": "latest"}, axis="columns")
        )
        toc_df = toc_df.assign(
            frequency=tlist["label"],
            earliest=t_labels["earliest"],
            latest=t_labels["latest"],
        )

    if variables:
        dimension_df = pd.json_normalize(df["dimension"], max_level=0)
        variables = (
            dimension_df[
                [
                    c
                    for c in dimension_df.columns
                    if "TLIST" not in c and "STATISTIC" not in c
                ]
            ]
            .stack()
            .str.get("label")
            .rename_axis(index=["id", "key"])
            .rename("label")
            .reset_index()
            .groupby("id")
            .agg({"label": lambda x: list(x)})
            .squeeze()
        )
        toc_df = toc_df.assign(variables=variables)

    return toc_df


# %%
def get_data(
    table: str,
    dimensions: list = None,
    excluded_dimensions: list = ["id", "size", "role"],
):
    """Given a CSO PxStat table name, return dataframe with all table data.
    With optional `dimensions` list, return dimensions. Otherwise, return all non-excluded dimensions.
    Some dimensions seem to be for internal use by PxStat and should be excluded.
    """
    url = f"https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadDataset/{table}/JSON-stat/2.0/en"
    try:
        json_data = requests.get(url).json()
    except requests.exceptions.SSLError:  # needed if behind firewall
        json_data = requests.get(url, verify=False).json()
    dimension_dict = {
        key: value["category"]["label"].values()
        for key, value in json_data["dimension"].items()
        if key not in excluded_dimensions
    }
    # Use cartesian product of dimension values to make a MultiIndex
    df_index = pd.MultiIndex.from_product(dimension_dict.values())
    df_index.names = dimension_dict.keys()

    # Add the dimensions to the actual data points and make into normal columns
    df = pd.DataFrame(json_data["value"], columns=["Value"], index=df_index)
    if dimensions is not None:
        df = df[dimensions + ["Value"]]
    return df


# %%
def life_table(
    statistics: list | str = "px",
    vintage: int = 2016,
) -> pd.Series:

    life_table = get_data("VSA32")
    life_table.columns = ["statistic", "vintage", "sex", "age", "value"]

    statistic_list = [statistics.lower()] if isinstance(statistics, str) else statistics
    life_table = life_table.loc[
        (life_table["statistic"].isin(statistic_list))
        & (life_table["vintage"] == f"{vintage}"),
        ["statistic", "sex", "age", "value"],
    ]
    life_table["age"] = life_table["age"].str.extract("(\d+)").astype("Int64").fillna(0)
    return life_table


# %%
def cpi(
    start_month: str = "1984M04",
    statistic: str = "Consumer Price Index (Base Dec 2001=100)",
    items: str | list = "All items",
    # normalize_to_most_recent=True,
) -> pd.Series:

    item_list = [items] if isinstance(items, str) else items

    print(item_list)
    cpi = get_data("CPM01")
    cpi.columns = ["statistic", "month", "item", "value"]
    # print(cpi.loc[cpi["item"].isin(["All items"]), "item"].value_counts())
    cpi = cpi.loc[
        (cpi["statistic"] == statistic)
        & (cpi["item"].isin(item_list))
        & (cpi["month"] >= start_month),
        # ["month", "item", "value"],
    ]
    cpi["month"] = pd.to_datetime(cpi["month"], format="%YM%m")
    # cpi = cpi.set_index("month").squeeze().rename("cpi")

    return cpi


# cso_pxstat_data("LRM02")

# # %%

# def test__cso_statbank_data__LRM02():
#     """Live Register (LR) total for test month should be same as published on CSO website
#     """

#     data = cso_pxstat_data(
#         table="LRM02", dimensions=["Age Group", "Sex", "Month", "Statistic",]
#     )

#     # Look up test data on CSO website
#     # https://www.cso.ie/en/releasesandpublications/er/lr/liveregistermarch2020/
#     test_month = "2020M03"
#     total_live_register = 205_209

#     results = data.loc[
#         (data["Month"] == test_month)
#         & (data["Age Group"] == "All ages")
#         & (data["Sex"] == "Both sexes")
#         & (data["Statistic"] == "Persons on the Live Register (Number)")
#     ]
#     assert int(results["Value"]) == total_live_register


# test__cso_statbank_data__LRM02()

# # %%

# def test__cso_statbank_data__QLF18():
#     """Labour force total for test quarter should be same as published on CSO website
#     """

#     data = cso_statbank_data(
#         table="QLF18", dimensions=["Age Group", "Sex", "Quarter", "Statistic",]
#     )

#     # Look up test data on CSO website
#     # https://www.cso.ie/en/releasesandpublications/er/lfs/labourforcesurveylfsquarter42019/
#     test_quarter = "2019Q4"
#     total_labour_force = 2_471_700

#     results = data.loc[
#         (data["Quarter"] == test_quarter)
#         & (data["Age Group"] == "15 years and over")
#         & (data["Sex"] == "Both sexes")
#         & (
#             data["Statistic"]
#             == "Person aged 15 years and over in the Labour Force (Thousand)"
#         )
#     ]
#     # Multiply by 1,000 here to match the number on CSO release
#     assert int(results["Value"] * 1_000) == total_labour_force
# %%
