# dbt-stonks

This is a pet project. The goal is to use the [RealStonks API](https://github.com/amansharma2910/RealStonks) along with some Wikipedia Webscraping on the S&P 500 to get stock data on those stocks,
then load them into BigQuery.

This job is meant to be run twice an hour every weekday while the NASDAQ is open. Official trading
hours for the NASDAQ are Monday - Friday from 9:30 AM to 4 PM US Eastern time, so it should run
13 times per weekday. 

Eventually, a dbt project will be developed to allow for some data transformations on the raw 
data that is initially loaded by the .py script.