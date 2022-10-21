# CSO Ireland Data
==============================

Easily download data from the CSO PxStat API as Pandas datasets.

Uses [requests-cache](https://github.com/requests-cache/requests-cache) for super fast access to cached requests and easy persistence with multiple storage backends.

## Installation
To install, just use pip:
```bash
pip install cso-ireland-data
```

## Usage
### Getting started
First, set up a `CSODataSession`. 

* By default, this is really simple.

    ```python
    from cso_data import CSODataSession
    cso = CSODataSession()
    ```

* If you want to add caching, no problem! All the functionality of [the `requests-cache` package](https://github.com/requests-cache/requests-cache) is available through `cached_session_params`.
    ```python
    from datetime import timedelta
    from cso_data import CSODataSession

    cso = CSODataSession(
        cached_session_params={
            "use_cache_dir": True,  # Save files in the default user cache dir
            "cache_control": True,  # Use Cache-Control response headers for expiration, if available
            "expire_after": timedelta(days=1),  # Otherwise expire responses after one day
        }
    )
    ```
* Stuck behind a corporate firewall that causes SSL certificate issues? Also no problem! All the functionality of [the `requests` `get()` method](https://requests.readthedocs.io/en/latest/user/quickstart/) is available through `request_params`.

    ```python
    from cso_data import CSODataSession

    # Tell requests.get() it's ok not to verify SSL certificates when getting data.
    # !!! Only do this if you're absolutely sure it's what you need !!!
    cso = CSODataSession(request_params={"verify": False})
    ```
### Getting the data catalogue
To get a catalogue (Table of Contents) of all the datasets that are available through the API, use `get_toc()`.

NB Requests for the ToC sometimes time out on the CSO API. IF this happens, try again!


```python
cso.get_toc()
```

| table_id   | table_name                                           | last_updated              | copyright                          | exceptional   | frequency   |   earliest |   latest | variables                                                          |
|:-----------|:-----------------------------------------------------|:--------------------------|:-----------------------------------|:--------------|:------------|-----------:|---------:|:-------------------------------------------------------------------|
| A0101      | 1996 Population and Percentage Change  1991 and 1996 | 2020-05-01 11:00:00+00:00 | Central Statistics Office, Ireland | False         | CensusYear  |       1996 |     1996 | ['Province County or City']                                        |
| A0102      | Population at Each Census Since 1841                 | 2020-05-01 11:00:00+00:00 | Central Statistics Office, Ireland | False         | CensusYear  |       1841 |     1996 | ['Province or County', 'Sex']                                      |
| A0103      | Population                                           | 2020-05-01 11:00:00+00:00 | Central Statistics Office, Ireland | False         | CensusYear  |       1996 |     1996 | ['Province County or City', 'Sex', 'Aggregate Town or Rural Area'] |
| A0104      | Population                                           | 2020-06-03 11:00:00+00:00 | Central Statistics Office, Ireland | False         | CensusYear  |       1996 |     1996 | ['Sex', 'Regional Authority']                                      |
| A0105      | 1996 Population and Percentage Change 1996 and 2002  | 2021-07-19 11:00:00+00:00 | Central Statistics Office, Ireland | False         | CensusYear  |       1996 |     1996 | ['Towns by Electoral Division']                                    |

### Getting a table using its ID code
To get the whole contents of a particular table hosted on the Statbank API, use `get_table()`.

You just need to know the ID code of the table, which you can look up using `get_toc()`.
```python
wpm29 = cso.get_table("WPM29")
wpm29.head()
```

|                           |   Wholesale Price Index (Excl VAT) for Energy Products |
|:--------------------------|-------------------------------------------------------:|
| ('Autodiesel', '2015M01') |                                                   96.7 |
| ('Autodiesel', '2015M02') |                                                  102   |
| ('Autodiesel', '2015M03') |                                                  103   |
| ('Autodiesel', '2015M04') |                                                  102.9 |
| ('Autodiesel', '2015M05') |                                                  104.6 |

### Getting some common tables quickly
The `CSODataSession` class includes some useful methods to get data from commonly accessed tables quickly. 

#### Monthly Consumer Price Index (CPI)
By default, the `cpi()` method returns a single column corresponding to the 'All items' headline CPI in the source table. 

Also by default, this index is re-normalized to the most recent month - you can toggle this by setting `normalize_to_most_recent` to `False`.

```python
simple_cpi = cso.monthly_cpi()
simple_cpi.tail()
```

| Month               |   All items |
|:--------------------|------------:|
| 2022-04-01 00:00:00 |      0.9725 |
| 2022-05-01 00:00:00 |      0.981  |
| 2022-06-01 00:00:00 |      0.9937 |
| 2022-07-01 00:00:00 |      0.9986 |
| 2022-08-01 00:00:00 |      1      |

It's also possible to pass a list of commodity groups:

```python
commmodity_group_cpi = cso.monthly_cpi(
    commodity_groups=[
        "All items",
        "Alcoholic beverages and tobacco",
        "Health",
        "Recreation and culture",
    ]
)
commodity_group_cpi.tail()
```

| Month               |   All items |   Alcoholic beverages and tobacco |   Health |   Recreation and culture |
|:--------------------|------------:|----------------------------------:|---------:|-------------------------:|
| 2022-04-01 00:00:00 |      0.9725 |                            0.9738 |   0.9828 |                   0.9945 |
| 2022-05-01 00:00:00 |      0.981  |                            0.9937 |   0.9851 |                   0.9954 |
| 2022-06-01 00:00:00 |      0.9937 |                            0.9958 |   0.9874 |                   0.9973 |
| 2022-07-01 00:00:00 |      0.9986 |                            0.9969 |   0.9874 |                   0.9991 |
| 2022-08-01 00:00:00 |      1      |                            1      |   1      |                   1      |1      |                   1      |

#### Live Register
Use the `live_register()` method to get Live Register numbers (optionally broken down by Age Group and Sex) by month. This is a long data series, starting in April 1967 and still continuing every month, so it may be convenient to specify a `start` and/or `end` date for the data returned.

The Live Register data series is based on a monthly point-in-time count of people who have active Jobseeker claims with the Department of Social Protection (DSP), and these counts are extracted from DSP's administrative computer systems on a particular day every month. 

Because of this, `live_register()` returns three possibly useful dates for each month:
1. 'Month' is the index of the data frame. It's just the last day of each calendar month.
2. 'reference_date' is the date of the point-in-time count of people with active Jobseeker claims. It's the last Friday of each month before May 2015, and the last Thursday of the month from then on.
3. 'extract_date' is the date on which the source administrative data was actually extracted - it's always the Sunday after the reporting_date.
   
```python
live_register = cso.live_register(start=datetime(2010, 1, 1))

```

|     | Month               | Age Group   | Sex        |   Persons on the Live Register |   Persons on the Live Register (Seasonally Adjusted) | reference_date      | extract_date        |
|----:|:--------------------|:------------|:-----------|-------------------------------:|-----------------------------------------------------:|:--------------------|:--------------------|
| 516 | 2010-04-30 00:00:00 | All ages    | Both sexes |                         432657 |                                               440800 | 2010-04-30 00:00:00 | 2010-05-02 00:00:00 |
| 517 | 2010-08-31 00:00:00 | All ages    | Both sexes |                         466923 |                                               444000 | 2010-08-27 00:00:00 | 2010-08-29 00:00:00 |
| 518 | 2010-12-31 00:00:00 | All ages    | Both sexes |                         437079 |                                               446000 | 2010-12-31 00:00:00 | 2011-01-02 00:00:00 |
| 519 | 2010-02-28 00:00:00 | All ages    | Both sexes |                         436956 |                                               439000 | 2010-02-26 00:00:00 | 2010-02-28 00:00:00 |
| 520 | 2010-01-31 00:00:00 | All ages    | Both sexes |                         436936 |                                               439400 | 2010-01-29 00:00:00 | 2010-01-31 00:00:00 |
#### Life Tables
The `life_table()` method by default returns a complete life table for the most recent source data vintage.

```python
life_table = cso.life_table()
life_table.head()
```

|               |     Ix |   dx |       px |       qx |    Lx |          Tx |   e0x |
|:--------------|-------:|-----:|---------:|---------:|------:|------------:|------:|
| ('Female', 0) | 100000 |  304 | 0.99696  | 0.00304  | 99848 | 8.34235e+06 | 83.42 |
| ('Female', 1) |  99696 |   22 | 0.999784 | 0.000216 | 99685 | 8.2425e+06  | 82.68 |
| ('Female', 2) |  99674 |    7 | 0.999933 | 6.7e-05  | 99671 | 8.14281e+06 | 81.69 |
| ('Female', 3) |  99668 |    5 | 0.999948 | 5.2e-05  | 99665 | 8.04314e+06 | 80.7  |
| ('Female', 4) |  99663 |    6 | 0.999936 | 6.4e-05  | 99659 | 7.94348e+06 | 79.7  |

