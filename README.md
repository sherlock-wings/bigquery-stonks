# bigquery-stonks

This is a pet project. The goal is to use the [RealStonks API](https://github.com/amansharma2910/RealStonks) along with some Wikipedia Webscraping on the S&P 500 to get stock data on those stocks,
then load them into BigQuery.



## Contents

- `get_stonks.py` -> This is a Python routine that physically calls the API and uploads the resulting data to BigQuery. This job is meant to be run twice an hour every day while the NASDAQ is open. Official trading hours for the NASDAQ are Monday to Friday from 9:30 AM to 4 PM US Eastern time, so it should run 14 times per weekday. 

- `dbt-stonks/*` -> This is the dbt project that will be used run transformations on the raw data uploaded by `get_stonks.py`.

All data is hosted by Google's [BigQuery](https://cloud.google.com/bigquery?utm_source=google&utm_medium=cpc&utm_campaign=emea-dk-all-en-dr-bkws-all-all-trial-e-gcp-1011340&utm_content=text-ad-none-any-dev_c-cre_574684121243-adgp_Hybrid%20%7C%20BKWS%20-%20EXA%20%7C%20Txt%20~%20Data%20Analytics%20~%20BigQuery-kwid_43700060419994235-kwd-139365086442-userloc_20243&utm_term=kw_big%20query%20google-net_g-plac_&&gad=1&gclid=Cj0KCQjwwISlBhD6ARIsAESAmp6FCVVFnyQHF3Gb8XfugTJr88lG6_T9lnrSOM9gzt4dxmsyyCCWUzUaAhzdEALw_wcB&gclsrc=aw.ds) services.