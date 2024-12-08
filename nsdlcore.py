import os
import io
import time
import zipfile
import numpy as np
import pandas as pd
import datetime as dt

from selenium import webdriver

BASE_DIRECTORY_NSDL=r"NSDLDATA"
DIRECTORY_NSDL_FII_EQUITY_FILE=os.path.join(BASE_DIRECTORY_NSDL, "FIIEQUITY.zfs")
DIRECTORY_NSDL_FII_DERIVATIVE_FILE=os.path.join(BASE_DIRECTORY_NSDL, "FIIDERIVATIVE.zfs")

os.makedirs(BASE_DIRECTORY_NSDL, exist_ok=True)

HEADLESS=True

def SeleniumNSDLUpdateInstituteData() -> bool:
    if(os.path.exists(DIRECTORY_NSDL_FII_EQUITY_FILE) and os.path.exists(DIRECTORY_NSDL_FII_DERIVATIVE_FILE)):
        with zipfile.ZipFile(DIRECTORY_NSDL_FII_EQUITY_FILE, "r") as ZFILE:
            START=dt.datetime.fromordinal(int(ZFILE.filelist[-1].filename))
    else:
        START=dt.datetime(2012, 12, 1)

    END=dt.date.today()
    MONTHRANGE=pd.date_range(START, END, freq="ME").to_list()
    MONTHRANGE.append(pd.Timestamp(END))

    config=webdriver.FirefoxOptions()
    if(HEADLESS):
        config.add_argument("--headless")
    driver=webdriver.Firefox(options=config)
    driver.implicitly_wait(600)
    driver.get(r"https://www.fpi.nsdl.co.in/web/Reports/Archive.aspx")

    for MONTH in MONTHRANGE:
        driver.execute_script(f"document.getElementById('hdnDate').setAttribute('value', '{MONTH.strftime('%d-%b-%Y')}')")
        driver.execute_script(f"__doPostBack('btnSubmit1','')")

        while(driver.execute_script("return document.readyState")!="loading"):
            time.sleep(1/100)
        while(driver.execute_script("return document.readyState")!="complete"):
            time.sleep(1/100)
        
        DFLIST=pd.read_html(io.BytesIO(driver.page_source.encode()))
        if(len(DFLIST)==1):
            continue

        EQUITYDF=DFLIST[1]
        EQUITYDF=EQUITYDF.drop(EQUITYDF.columns[-1], axis=1)
        EQUITYDF.columns=['date', 'type', 'route', 'buy', 'sell', 'removeone', 'removetwo', "usdinr"]
        EQUITYDF=EQUITYDF.drop(["removeone", "removetwo"], axis=1)
        EQUITYDF=EQUITYDF[~(EQUITYDF.T.nunique().isin([0, 1]))]
        EQUITYDF=EQUITYDF[~(EQUITYDF["date"].str.contains("Total"))]
        EQUITYDF=EQUITYDF[~EQUITYDF["route"].isin(["Total", "Sub-total"])]
        EQUITYDF["usdinr"]=EQUITYDF["usdinr"].str[3:].astype(np.float64)
        EQUITYDF[["buy", "sell"]]=EQUITYDF[["buy", "sell"]].astype(np.float64)*1_00_00_000
        EQUITYDF["date"]=pd.to_datetime(EQUITYDF["date"], format="%d-%b-%Y")
        EQUITYDF=EQUITYDF[EQUITYDF["date"]>START]

        DERIVATIVEDF=DFLIST[2]
        DERIVATIVEDF.columns=['date', 'type', 'buycontract', 'buyvalue', 'sellcontract', 'sellvalue', "oicontract", "oivalue"]
        DERIVATIVEDF=DERIVATIVEDF[~(DERIVATIVEDF.T.nunique().isin([0, 1]))]
        DERIVATIVEDF=DERIVATIVEDF[~(DERIVATIVEDF["date"].str.contains("Total"))]
        DERIVATIVEDF[['buycontract', 'buyvalue', 'sellcontract', 'sellvalue', "oicontract", "oivalue"]]=DERIVATIVEDF[['buycontract', 'buyvalue', 'sellcontract', 'sellvalue', "oicontract", "oivalue"]].astype(np.float64)
        DERIVATIVEDF["date"]=pd.to_datetime(DERIVATIVEDF["date"], format="%d-%b-%Y")
        DERIVATIVEDF[['buyvalue', 'sellvalue', 'oivalue']]=DERIVATIVEDF[['buyvalue', 'sellvalue', 'oivalue']]*1_00_00_000
        DERIVATIVEDF=DERIVATIVEDF[DERIVATIVEDF["date"]>START]

        with zipfile.ZipFile(DIRECTORY_NSDL_FII_EQUITY_FILE, "a", zipfile.ZIP_STORED) as ZFILE:
            for DATE in EQUITYDF["date"].unique():
                with ZFILE.open(f"{DATE.toordinal()}", "w") as WZFILE:
                    WZFILE.write(EQUITYDF[EQUITYDF["date"]==DATE].to_parquet(index=False))
        
        with zipfile.ZipFile(DIRECTORY_NSDL_FII_DERIVATIVE_FILE, "a", zipfile.ZIP_STORED) as ZFILE:
            for DATE in EQUITYDF["date"].unique():
                with ZFILE.open(f"{DATE.toordinal()}", "w") as WZFILE:
                    WZFILE.write(DERIVATIVEDF[DERIVATIVEDF["date"]==DATE].to_parquet(index=False))
                print(DATE.strftime('%d-%B-%Y'))

    driver.close()

    return True

if __name__=="__main__":
    print("\x1b[31m\x1b[1mYou wouldn't do that, Specifically!\x1b[0m")