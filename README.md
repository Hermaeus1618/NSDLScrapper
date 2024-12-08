# FOREIGN INSTITUTIONAL INVESTOR NSDL

A Selenium approach for scrapping FII trading data from https://www.fpi.nsdl.co.in.

I am only uploading algorithm for data fetching for the time being.
Data is filtered to remove any 'mid-table totaling' so that it can be use for a larger timeframe.

This approach is timeout error proof in most cases.

Speed of this algorithm is quite high because instead of using selenium functions for navigation it executes javascript using `webdriver.Firefox.execute_script()` directly from browsers API.

Data can be combined from ZIP by:
```python
import polars as pl
MAXDATE=3600
DFLIST=[]
with zipfile.ZipFile(NSDLCore.DIRECTORY_NSDL_FII_EQUITY_FILE, "r") as ZFILE:
    DATERANGE=[int(F.filename) for F in ZFILE.infolist()][-MAXDATE:]
    for i in range(len(DATERANGE)):
        with ZFILE.open(f"{DATERANGE[i]}", "r") as WZFILE:
            XDF=pl.read_parquet(io.BytesIO(WZFILE.read()))
            DFLIST.append(XDF)
DF=pl.concat(DFLIST)
```

In this example i have used polars which is significantly faster than pandas in this algorithm but you can also use pandas simply as:

```python
import pandas as pd
with zipfile.ZipFile(NSDLCore.DIRECTORY_NSDL_FII_EQUITY_FILE, "r") as ZFILE:
    DATERANGE=[int(F.filename) for F in ZFILE.infolist()][-MAXDATE:]
    for i in range(len(DATERANGE)):
        with ZFILE.open(f"{DATERANGE[i]}", "r") as WZFILE:
            XDF=pd.read_parquet(io.BytesIO(WZFILE.read()))
            DFLIST.append(XDF)
DF=pd.concat(DFLIST)
```

Derivatives section is currently under debugging after major NSDL server update.
