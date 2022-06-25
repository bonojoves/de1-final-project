
# Nickel Scraper 

An ETL Pipeline designed by Sandino Joves to scrape data from the ff. sources:
1. RSS feeds of Business World and Business Network
2. NIKL data from the **fastquant** package (https://github.com/enzoampil/fastquant)
3. Weather data from **meteostat** (https://dev.meteostat.net/guide.html#our-services)

# Usage
1. (VM instance) clone the repository via onto your VM instance then navigate to the **de1-final-project** directory.
2. Make sure that docker is installed (https://docs.docker.com/engine/install/debian/) - in GCE's case, we are running a Debian terminal
3. For the first instance, run *docker compose up airflow-init* to initialize docker and airflow
4. Then run *docker compose up*
5. Once the Airflow webserver is up, you are now able to access the DAGs

# ETL Tasks
## Extract
1. RSS feeds are extracted via **feedparser**
2. NIKL stock data was chosen due to them running one of the biggest nickel mines in the PH in Surigao. The developer is also familiar with it due to his brief trading stint. The **fastquant** package is able to gather data in two lines of code (kindly see link above)
3. Weather data is gathered via **meteostat**. Used Cagayan De Oro lat-long due to Surigao lat-long returning a blank data frame.

## Transform
> NIKL stock data and weather data are combined into 1 data frame, merging via the common date column. Any NAs are filled with the medians for that column.
> This merged data frame is then normalized with the intention of graphing the data and checking for correlation between the weather and stock data.

## Load
> Tasks are loaded to the GCP bucket, then the cache is cleaned of any files to prevent any duplicates being uploaded.

# Future Considerations / challenges encountered
1. Incorporate actual export data instead of stock price - export data sources are not free
2. Add more business news sources
3. Need more weather data to paint a full picture - meteostat gives stats like temperature, wind speed and wind direction; need to supplement with more

