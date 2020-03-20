 ### Collector for HDRO's Datasets
[![Build Status](https://travis-ci.org/OCHA-DAP/hdx-scraper-hdro.svg?branch=master&ts=1)](https://travis-ci.org/OCHA-DAP/hdx-scraper-hdro) [![Coverage Status](https://coveralls.io/repos/github/OCHA-DAP/hdx-scraper-hdro/badge.svg?branch=master&ts=1)](https://coveralls.io/github/OCHA-DAP/hdx-scraper-hdro?branch=master)

This script connects to the [HDRO API](http://hdr.undp.org/en/content/human-development-report-office-statistical-data-api) and extracts country by country creating a dataset per country in HDX. It makes one reads from HDRO and 1000 read/writes (API calls) to HDX in total. It creates around 2 temporary files per country of no more than 100Kb size. It is run every year. 


### Usage

    python run.py

For the script to run, you will need to have a file called .hdx_configuration.yml in your home directory containing your HDX key eg.

    hdx_key: "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
    hdx_read_only: false
    hdx_site: prod
    
 You will also need to supply the universal .useragents.yml file in your home directory as specified in the parameter *user_agent_config_yaml* passed to facade in run.py. The collector reads the key **hdx-scraper-hdro** as specified in the parameter *user_agent_lookup*.
 
 Alternatively, you can set up environment variables: USER_AGENT, HDX_KEY, HDX_SITE, EXTRA_PARAMS, TEMP_DIR, LOG_FILE_ONLY
