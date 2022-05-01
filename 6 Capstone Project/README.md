# CapstoneProject: Enriching US Immigration Data with Further Data Sources

This project's goal is to further enrich US I94 immigration data with airports, temperature and demographics data for have a broader data spectrum for analysis. The complete process is laid out in the Jupyter notebook "Capstone Project.ipynb".

## 1 Data Sources


### Data Set 1: US I94 Immigration Data
**Source**: [https://travel.trade.gov/research/reports/i94/historical/2016.html](https://travel.trade.gov/research/reports/i94/historical/2016.html)

This data comes from the US National Tourism and Trade Office and includes all different kinds of information about US immigrants.

### Data Set 2: Earth Surface Temperature Data
**Source**: [https://www.kaggle.com/datasets/berkeleyearth/climate-change-earth-surface-temperature-data](https://www.kaggle.com/datasets/berkeleyearth/climate-change-earth-surface-temperature-data)

World temperature data provided by Kaggle - dataset pre-filtered for US to make upload to GitHub possible.

### Data Set 3: U.S. City Demographic Data
**Source**: [https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/)

This dataset contains information about the demographics of all US cities and census-designated places with a population greater or equal to 65,000 and is derived from the US Census Bureau's 2015 American Community Survey.

### Data Set 4: Airport Codes
**Source**: [https://datahub.io/core/airport-codes#data](https://datahub.io/core/airport-codes#data)

This is a simple table of airport codes and corresponding cities.

## 2 Cleaning Steps

* Remove non-existing airport codes from i94 immigration data
* Drop various cols containing many NaN values
* Drop residual rows containing NaN values

## 3 Defining the Data Model

A star schema is used for data modeling.

| Table name | Columns | Description | Table type |
| ------- | ---------- | ----------- | ---- |
| immigration | cicid - year - month - cit - res - iata - arrdate - mode - addr - depdate - bir - visa - coun- dtadfil -  entdepa - entdepd - entdepu - matflag - biryear - dtaddto - gender - airline - admnum - fltno - visatype | I94 immigrations data | Fact table |
| airports | iata_code - name - type - local_code - coordinates - city | Information related to airports | Dimension table |
| demographics | city - state - media_age - male_population - female_population - total_population - num_veterans - foreign_born - average_household_size - state_code - race - count | Demographics data for cities | Dimension table |
| temperature | timestamp - average_temperature - average_temperatur_uncertainty - city - country - latitude - longitude | Temperature information | Dimension table |
