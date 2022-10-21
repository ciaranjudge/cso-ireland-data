# %%
from dataclasses import dataclass, field, InitVar
from datetime import datetime
from dateutil.relativedelta import TH, FR
from requests_cache import CachedSession

import numpy as np
import pandas as pd
from pandas.tseries.holiday import DateOffset

# TODO Tests!!!

# %%
def jsonstat_toc_to_df(
    json_data: dict,
    show_frequency: bool = True,
    show_variables: bool = True,
    show_url: bool = False,
) -> pd.DataFrame:
    """
    Convert a JSONStat-formatted table of contents (ToC) into a Pandas DataFrame.

    A JSONStat ToC provides metadata for all the tables in a data repository, such as CSO's Open Data Portal PxStat.
    The live PxStat ToC is available at https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadDataset/.

    Parameters
    ----------
    json_data : dict
        A valid Collection of JsonStat items.
        For more details, see https://github.com/CSOIreland/PxStat/wiki/API-Cube-RESTful.
    show_frequency : bool = True
        If this is True, the output DataFrame will include "frequency", "earliest", and "latest" columns.
    show_variables : bool = True
        If this is True, the output DataFrame will include a "variables" column.
    show_url : bool = False
        If this is True, the output DataFrame will include a "url" column.
        Excluded by default as all table URLS are identical except for table ID!

    Returns
    -------
    DataFrame:
        Index:
            "table_id" : object [str]
                All valid table IDs listed sequentially by id number, e.g. A0101, A0102, A0103.
        Columns:
            "table_name" : object [str]
                Description of the table. Not necessarily unique!!
            "last_updated" : datetime64[ns, UTC]
            "copyright" : object [str]
                Name of the organisation which owns the table.
            "exceptional" : bool
                If this is True, it means the table is experimental or not regularly produced.
            "frequency" : object [str]
                Description of the time periods covered by the table, e.g. 'Year', 'Month', 'Quarter'.
                Only included if `show_frequency` is True.
            "earliest" : object [str]
                Earliest time period covered by this table, e.g. "2011", "1975M01".
                Only included if `show_frequency` is True.
            "latest" : object [str]
                Latest time period covered by this table, e.g. "2021", "2022M92".
                Only included if `show_frequency` is True.
            "variables" : object [list[str]]
                A list of the variables (dimensions) for each table.
                Only included if `show_frequency` is True.
            "url" : object [url]
                Direct JSONStat link to this table.
                Only included if `show_frequency` is True.

    See Also
    --------
    CSODataSession.get_toc : Wrapper around `jsonstat_toc_to_df` that directly retrieves the ToC from CSO's PxStat portal.
    jsonstat_table_to_df : Converts JSONStat-formatted table data into a DataFrame.
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
        copyright=df["extension"].str.get("copyright").str.get("name"),
        exceptional=df["extension"].str.get("exceptional"),
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

    if show_url:
        toc_df = toc_df.assign(url=df["href"])

    return toc_df.set_index("table_id").sort_index()


def jsonstat_table_to_df(json_data, metadata: bool = False):
    """
    Convert a dict representing a JSONStat-formatted PxStat table into a Pandas DataFrame.

    Parameters
    ----------
    json_data : dict
        A dict representing a valid JSONStat-formatted PxStat table.
    metadata: bool = False
        If metadata is set to True, include available table metadata in the output dataframe.
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


def live_register_dates(start=datetime(1967, 1, 1), end=datetime.now()):
    """
    Returns months LR reference dates, and corresponding 'ISTS' (administrative data) extract dates,
    for a date range from `start` to `end`.
    LR reference date was last Friday of each month before May 2015, and last Thursday from then on.
    Administrative data extract date is always the Sunday after the LR reporting date.
    """
    lr_dates = pd.DataFrame(
        {"reference_date": pd.NaT, "extract_date": pd.NaT},
        index=pd.date_range(start, end, freq="M", name="month"),
    )

    # Reference date was last Friday of each month before May 2015
    # and last Thursday of each month from then on.
    before_may_2015 = lr_dates.index < datetime(2015, 5, 1)
    lr_dates["reference_date"] = np.where(
        before_may_2015,
        lr_dates.index + DateOffset(weekday=FR(-1)),
        lr_dates.index + DateOffset(weekday=TH(-1)),
    )
    # Extract date is always the Sunday after the reference date
    # so add 2 days to the reference date for months before May 2015 (Fri to Sun)
    # and add 3 days for months from then on (Thu to Sun).
    lr_dates["extract_date"] = np.where(
        before_may_2015,
        lr_dates["reference_date"] + DateOffset(days=2),
        lr_dates["reference_date"] + DateOffset(days=3),
    )
    return lr_dates


def live_register_months_to_datetime(months: pd.Series):
    return pd.to_datetime(months, infer_datetime_format=True) + pd.offsets.MonthEnd()


# %%
@dataclass
class CSODataSession:
    cached_session_params: InitVar[dict | None] = None
    request_params: dict = field(default_factory=dict)
    session: CachedSession = field(init=False)
    """
    Creates a session that connects to CSO PxStat and enables downloading PxStat tables.

    Parameters
    ----------   
    cached_session_params: dict
        An optional dict of cached session parameters such as data backend and cache lifetime.
        Key-value pairs here are passed through to the CachedSession that manages the CSO PxStat session.
        A full list of possible cached session parameters is given here:
        https://requests-cache.readthedocs.io/en/stable/session.html

    request_params: dict
        An optional dict of request parameters. 
        Any key-value pairs here are passed through to the underlying session.get() call.
        A full list of possible request parameters is given here:
        https://requests.readthedocs.io/en/latest/api/

    
    Attributes
    ----------
    session: CachedSession
        A CachedSession object that manages the session and caching behaviour of the CSODataSession.
        Normally initialised by passing cached_session_params when constructing the CSODataSession,
        but can also be accessed directly, e.g. to clear cached data (`cso.session.cache.clear()`).
        A full list of possible cached session parameters, attributes, and methods is given here:   
        https://requests-cache.readthedocs.io/en/stable/session.html
    
    Examples
    --------
    >>> from cso_data import CSODataSession
    >>> # Normal setup, no need to specify request_params
    >>> cso = CSODataSession()
    >>> # Alternative setup behind corporate firewall
    >>> cso = CSODataSession(request_params={"verify": False})
    """

    def __post_init__(self, cached_session_params):
        self.session = (
            CachedSession(**cached_session_params)
            if cached_session_params
            else CachedSession()
        )

    def get_toc(
        self,
        show_frequency: bool = True,
        show_variables: bool = True,
        show_url: bool = False,
    ) -> pd.DataFrame:
        """
        Get the table of contents (TOC) of all available PxStat tables as a dataframe.
        NB The CSO PxStat website sometimes fails to return the requested data.
        If this happens, retry this request.
        The live PxStat ToC is available at https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadDataset/.

        Parameters
        ----------
        json_data : dict
            A valid Collection of JsonStat items.
            For more details, see https://github.com/CSOIreland/PxStat/wiki/API-Cube-RESTful.
        show_frequency : bool = True
            If this is True, the output DataFrame will include "frequency", "earliest", and "latest" columns.
        show_variables : bool = True
            If this is True, the output DataFrame will include a "variables" column.
        show_url : bool = False
            If this is True, the output DataFrame will include a "url" column.
            Excluded by default as all table URLS are identical except for table ID!

        Returns
        -------
        DataFrame:
            Index:
                "table_id" : object [str]
                    All valid table IDs listed sequentially by id number, e.g. A0101, A0102, A0103.
            Columns:
                "table_name" : object [str]
                    Description of the table. Not necessarily unique!!
                "last_updated" : datetime64[ns, UTC]
                "copyright" : object [str]
                    Name of the organisation which owns the table.
                "exceptional" : bool
                    If this is True, it means the table is experimental or not regularly produced.
                "frequency" : object [str]
                    Description of the time periods covered by the table, e.g. 'Year', 'Month', 'Quarter'.
                    Only included if `show_frequency` is True.
                "earliest" : object [str]
                    Earliest time period covered by this table, e.g. "2011", "1975M01".
                    Only included if `show_frequency` is True.
                "latest" : object [str]
                    Latest time period covered by this table, e.g. "2021", "2022M92".
                    Only included if `show_frequency` is True.
                "variables" : object [list[str]]
                    A list of the variables (dimensions) for each table.
                    Only included if `show_variables` is True.
                "url" : object [url]
                    Direct JSONStat link to this table.
                    Only included if `show_url` is True.

        Examples
        --------
        >>> from cso_data import CSODataSession
        >>> cso = CSODataSession()
        >>> toc = cso.jsonstat_toc_to_df(json_data)
        >>> toc.info()
        >>> # It's easy to look up any table if you know its ID:
        >>> toc.loc["VSA32"]
        table_name                  Period Life Expectancy
        last_updated             2020-09-29 11:00:00+00:00
        copyright       Central Statistics Office, Ireland
        exceptional                                  False
        frequency                                     Year
        earliest                                      2002
        latest                                        2016
        variables                             [Age x, Sex]
        Name: VSA32, dtype: object
        >>> # Or to look up tables by name:
        >>> toc.loc[toc["table_name"].str.contains("Period Life Expectancy"), ["table_name", "variables"]]
                table_name  variables
        table_id
        VSA30	Period Life Expectancy at Various Ages	[Age, Sex]
        VSA31	Period Life Expectancy	[Age, Sex, Region]
        VSA32	Period Life Expectancy	[Age x, Sex]
        VSA33	Period Life Expectancy	[Age Group, Country, Sex]
        VSA34	Period Life Expectancy at Various Ages	[Age, Country, Sex]

        """
        url = "https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadCollection"
        json_data = self.session.get(url, **self.request_params).json()
        return jsonstat_toc_to_df(json_data, show_frequency, show_variables)

    def get_table(self, table: str, metadata: bool = False) -> pd.DataFrame:
        """
        Given a CSO PxStat table name, get table data from PxStat API and return dataframe with all table data.

        Parameters
        ----------
        table: str
            The identity code of the requested table.

        metadata: bool = False
            If metadata is set to True, include available table metadata in the output dataframe.
        """
        url = f"https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadDataset/{table}/JSON-stat/2.0/en"
        json_data = self.session.get(url, **self.request_params).json()
        return jsonstat_table_to_df(json_data, metadata)

    def life_table(
        self,
        statistics: str | list = ["Ix", "dx", "px", "qx", "Lx", "Tx", "e0x"],
        vintage: int | str = "most_recent",
    ) -> pd.DataFrame | pd.Series:
        """
        Return a life table with data from CSO's PxStat databank (table VSA32).

        Parameters
        ----------
        statistics: str | list
            The statistics to be returned from the life table.
            Default is ["Ix", "dx", "px", "qx", "Lx", "Tx", "e0x"].

        vintage: str
            Choose a life table vintage.
            Possible values are:
                "most_recent" (default): returns the most recent data vintage year
                "all": returns all vintage years
                or any valid vintage year.

        Returns
        -------
        pandas.DataFrame | pandas.Series
            If `statistics` has more than one element, returns a DataFrame with statistics as columns.
            Otherwise, returns a Series.
            If `vintage` is a single year, index is ["Sex", "Age x"].
            If `vintage` is "all", index is ["Year", "Sex", Age x"].
        """

        statistic_list = [statistics] if isinstance(statistics, str) else statistics

        life_table = self.get_table("VSA32").reset_index()
        life_table["Age x"] = (
            life_table["Age x"].str.extract("(\d+)").astype("Int64").fillna(0)
        )
        life_table = life_table.set_index(["Year", "Sex", "Age x"]).sort_index()

        if vintage != "all":
            if vintage == "most_recent":
                life_table = life_table.loc[
                    life_table.index.get_level_values("Year").max()
                ]
            else:
                life_table = life_table.loc[str(vintage)]

        return life_table[statistics]

    def monthly_cpi(
        self,
        start_month: str | None = None,
        statistic: str = "Consumer Price Index (Base Dec 2001=100)",
        commodity_groups: str | list = "All items",
        normalize_to_most_recent=True,
    ) -> pd.DataFrame:
        """
        Produces a time series of monthly CPI from CSO PxStat databank (table CPM01).

        You can choose as many commodity groups as you want, but you have to pick just one statistic.
        """

        commodity_group_list = (
            [commodity_groups]
            if isinstance(commodity_groups, str)
            else commodity_groups
        )

        cpi = self.get_table("CPM01")
        cpi = (
            cpi.loc[
                cpi.index.get_level_values("Commodity Group").isin(
                    commodity_group_list
                ),
                statistic,
            ]
            .unstack(level="Commodity Group")
            .reset_index()
            .assign(Month=lambda x: pd.to_datetime(x["Month"], infer_datetime_format=True))
            .set_index("Month")
            .sort_index()          
        )
        if normalize_to_most_recent:
            most_recent = cpi.loc[cpi.index.max()]
            cpi = cpi / most_recent

        return cpi[commodity_group_list]

    # cpi = monthly_cpi(
    #     commodity_groups=[
    #         "Alcoholic beverages and tobacco",
    #         "All items",
    #         "Clothing and footwear",
    #         "Communications",
    #     ],
    #     verify=False,
    # )

    # cso_pxstat_data("LRM02")

    def live_register(
        self,
        start: datetime = datetime(1967, 1, 1),
        end: datetime = datetime.now(),
        age_groups: list = ["All ages"],
        sexes: list = ["Both sexes"],
    ) -> pd.DataFrame | pd.Series:
        """
        Produce Live Register data broken down by age group and sex.
        """
        lr = self.get_table("LRM02")
        lr_dates = live_register_dates()
        lr = (
            lr.loc[
                (lr.index.get_level_values("Age Group").isin(age_groups))
                & (lr.index.get_level_values("Sex").isin(sexes))
            ]
            .reset_index()
            .assign(
                Month=(
                    lambda x: pd.to_datetime(x["Month"], infer_datetime_format=True)
                    + pd.offsets.MonthEnd()
                )
            )
            .merge(lr_dates, left_on="Month", right_index=True, how="inner")
        )

        return lr.loc[(start <= lr["Month"]) & (lr["Month"] <= end)]


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
