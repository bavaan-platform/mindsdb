# VNstock Handler

VNStock handler for MindsDB provides interfaces to connect to stock market  via APIs and pull stock data into MindsDB.

---

## Table of Contents

- [VNstock Handler](#vnstock-handler)
  - [Table of Contents](#table-of-contents)
  - [About VNstock](#about-vnstock)
  - [VNstock Handler Implementation](#vnstock-handler-implementation)
  - [Implemented Features](#implemented-features)
  - [Example Usage](#example-usage)

---

## About VNstock

Vnstock is an open source library provide finance information for Vietnamese

## VNstock Handler Implementation

This handler was implemented using [vnstock3](https://github.com/thinh-vu/vnstock) library.


## Implemented Features
  Support select stock resources
- [x] company_overview 
- [x] company_profile
- [x] company_shareholders
- [x] company_officers
- [x] company_susidiaries
- [x] company_dividends
- [x] company_events
- [x] company_news
- [x] finance_income_statement
- [x] finance_balance_sheet
- [x] finance_cash_flow 
- [x] finance_ratio
- [x] quoute_history
- [x] quoute_instraday
- [ ] quoute_fx
- [ ] quoute_crypto
- [ ] quoute_word_index
- [x] fund_list
- [x] fund_filter
- [x] fund_detail_nav_report
- [x] fund_detail_top_holding
- [x] fund_detail_industy_holding
- [x] fund_assest_holding
- [x] sjc_gold_price
- [x] btmc_gold_price 
- [x] exchange_rate 


## Example Usage

The first step is to create a database with the new `vnstock` engine.
~~~~sql
CREATE DATABASE vnstock_datasource
WITH ENGINE = 'vnstock',
PARAMETERS = {
};
~~~~
### Get all stock symbol
~~~~sql
SELECT * FROM vnstock_database.list_all_symbols;
~~~~
By exchange
~~~~sql
SELECT * FROM vnstock_database.list_all_symbols_by_exchange;
~~~~
By industy
~~~~sql
SELECT * FROM vnstock_database.listing_all_symbols_by_industries;
~~~~

### 1. Get company information

All query related to company need 2 parameter: `symbol` and `datasource` in where condition

~~~~sql
SELECT * FROM vnstock_datasource.company_overview
where symbol = 'VCI' and data_source='TCBS';
~~~~

~~~~sql
SELECT * FROM vnstock_datasource.company_profile
where symbol = 'VCI' and data_source='TCBS';
~~~~

~~~~sql
SELECT * FROM vnstock_database.company_dividends
where symbol='VCI' and data_source='TCBS' ;
~~~~

### 2. Get financial report information
When it come to financail report of each company, there need to be addition
parameters:
- `period`: `year` for yearly report, 'quarter' for quarter report.
There are optional param like: 
- `lang`(optional): change language of header. Support 'vi' for Viet Nam and 'en' for English
- `dropna`(optional): Remove all row that has NaN data
~~~~sql
SELECT * FROM vnstock_database.finance_income_statement
where symbol='VCI' and data_source='TCBS'and period='year';
~~~~
<!-- Currently, Only `data_source=VCI` support finance_ratio:
~~~~sql
SELECT * FROM vnstock_database.stock_finance_ratio
where symbol='VCI' and data_source='VCI'and period='year';
~~~~ -->

### 3. Get quote history

- `start`: start day of query. Format: YYYY-mm-dd
- `end`:  end day of query. Format: YYYY-mm-dd
- `interval`: frequency of data collected. . Including:
    
    * `1m`: 1 minutes
    
    * `5m`: 5 minutes
    
    * `15m`: 15 minutes
    
    * `30m`: 30 minutes
    
    * `1H`: 1 hour

    * `1D`: 1 day
    
    * `1W`: 1 week
    
    * `1M`: 1 month

  Default: `D`
- `to_df`: Cho phép kết quả truy vấn trả về là pandas DataFrame hay JSON. Mặc định là True trả về DataFrame, đặt là False nếu muốn nhận kết quả dưới dạng JSON.

Examples:

~~~~sql
SELECT * FROM vnstock_database.quote_history
where symbol='VCI' and data_source='TCBS' and date_start='2020-01-01'and date_end='2024-05-25';
~~~~

~~~~sql
SELECT * FROM vnstock_database.quote_intraday
where symbol='VCI' and data_source='TCBS' ;
~~~~


### 4. Get Gold price

Check price of sjc gold:
~~~~sql
SELECT * FROM vnstock_database.sjc_gold_price;
~~~~

Check gold price from Bao Tin minh Chau. 

~~~~sql
SELECT * FROM vnstock_database.exchange_rate;
~~~~

For exchange rate, there need to specify `date` to check. By default, the date will be set to today

~~~~sql
SELECT * FROM vnstock_database.exchange_rate
where date = '2024-11-07';
~~~~

### 5. Funding information

List all fund symbol:
~~~~sql
SELECT * FROM vnstock_database.fund_list;
~~~~
Report NAV:
~~~~sql
SELECT * FROM vnstock_database.fund_details_nav_report
where symbol='SSISCA';
~~~~
Get top holding:
~~~~sql
SELECT * FROM vnstock_database.fund_details_top_holding
where symbol='SSISCA';
~~~~
Get industry holding:
~~~~sql
SELECT * FROM vnstock_database.fund_details_industry_holding
where symbol='SSISCA';
~~~~
Get asset holding:
~~~~sql
SELECT * FROM vnstock_database.fund_details_industry_holding
where symbol='SSISCA';
~~~~