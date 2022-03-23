# %%
import requests
import pandas as pd

# TODO Docstrings!!
# TODO Tests!!!


def process_toc(
    json_data: dict, show_frequency: bool = True, show_variables: bool = True
) -> pd.DataFrame:
    """
    D
    O
    C
    S
    T
    R
    I
    N
    G
    !
    """
    df = pd.json_normalize(json_data["link"]["item"], max_level=0)

    toc_df = pd.DataFrame(index=df.index).assign(
        table_id=(
            df["href"]
            .str.removeprefix(
                "https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadDataset/"
            )
            .str.removesuffix("/JSON-stat/2.0/en")
        ),
        table_name=df["label"],
        last_updated=pd.to_datetime(df["updated"]),
        url=df["href"],
        copyright=df["extension"].str.get("copyright"),
    )

    if show_frequency or show_variables:
        dimension_df = pd.json_normalize(df["dimension"], max_level=0)

    if show_frequency:
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

    if show_variables:
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

    return toc_df.set_index("table_id")


def get_toc(
    show_frequency: bool = True, show_variables: bool = True, **requests_kwargs
) -> pd.DataFrame:
    """
    Doooooooooooooooooocccccccccccccstrrrriiiiiiiiiiiiiiiiinnnnnnnnnnnnngggggggg!
    """
    url = "https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadCollection"
    json_data = requests.get(url, **requests_kwargs).json()
    return process_toc(json_data, show_frequency, show_variables)


def process_table(json_data, metadata: bool = False):
    """
    Please write a docstring please please please
    Please
    Please
    Please
    Pleaaaaaaase!
    """
    # Create dictionary with list of category labels for each dimension label
    dimensions = {
        dimension["label"]: dimension["category"]["label"].values()
        for dimension in json_data["dimension"].values()
    }
    # Use cartesian product of dimension values to make a MultiIndex
    dimension_index = pd.MultiIndex.from_product(
        dimensions.values(), names=dimensions.keys()
    )

    id_labels = json_data["dimension"]["STATISTIC"]["category"]["label"]

    # Add the dimensions to the actual data points and unstack Statistic
    table = (
        pd.DataFrame(json_data["value"], columns=["Value"], index=dimension_index)
        .unstack("Statistic")
        .droplevel(0, axis="columns")
        .loc[:, [id_labels[id] for id in sorted(id_labels)]]
    )

    if metadata:
        id_units = json_data["dimension"]["STATISTIC"]["category"]["unit"]
        statistic_units = [
            (id_labels[id], id_units[id]["label"]) for id in sorted(id_labels)
        ]
        table.columns = pd.MultiIndex.from_tuples(
            statistic_units, names=["statistic", "unit"]
        )

    return table


def get_table(table: str, metadata: bool = False, **requests_kwargs) -> pd.DataFrame:
    """
    Given a CSO PxStat table name, get table data from PxStat API and return dataframe with all table data.
    Any keyword arguments given to this function are passed straight through to requests.get().
    For example, to disable ssl verification behind a corporate firewall, `verify=False`"""
    url = f"https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadDataset/{table}/JSON-stat/2.0/en"
    json_data = requests.get(url, **requests_kwargs).json()
    return process_table(json_data, metadata)


# %%
def life_table(vintage: int | str = "most_recent", **requests_kwargs) -> pd.Series:
    """
    Please just give me a docstring.
    Any docstring!!
    Seriously, just write something here.
    """

    life_table = get_table("VSA32", **requests_kwargs).reset_index()
    life_table["Age x"] = (
        life_table["Age x"].str.extract("(\d+)").astype("Int64").fillna(0)
    )
    life_table = life_table.set_index(["Year", "Sex", "Age x"]).sort_index()

    if vintage != "all":
        if vintage == "most_recent":
            life_table = life_table.loc[life_table.index.get_level_values("Year").max()]
        else:
            life_table = life_table.loc[str(vintage)]

    return life_table


# %%
# def monthly_cpi(
#     start_month: str | None = None,
#     statistic: str = "Consumer Price Index (Base Dec 2001=100)",
#     commodity_groups: str | list = "All items",
#     # normalize_to_most_recent=True,
# ) -> pd.Series:
#     """
#     D O
#     D O C
#     D O C String
#     DOCSTRING!!!
#     """

#     item_list = [commodity_groups] if isinstance(commodity_groups, str) else commodity_groups

#     cpi = get_table("CPM01")

#     cpi["month"] = pd.to_datetime(cpi["month"], format="%YM%m")
#     # print(cpi.loc[cpi["item"].isin(["All items"]), "item"].value_counts())
#     cpi = cpi.loc[
#         (cpi["statistic"] == statistic)
#         & (cpi["item"].isin(item_list))
#         & (cpi["month"] >= start_month if start_month is not None else ),
#         # ["month", "item", "value"],
#     ]

#     # cpi = cpi.set_index("month").squeeze().rename("cpi")

#     return cpi


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
