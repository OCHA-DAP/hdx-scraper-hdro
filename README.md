 ### Collector for HDRO's Datasets
[![Build Status](https://github.com/OCHA-DAP/hdx-scraper-hdro/workflows/build/badge.svg)](https://github.com/OCHA-DAP/hdx-scraper-hdro/actions?query=workflow%3Abuild) [![Coverage Status](https://coveralls.io/repos/github/OCHA-DAP/hdx-scraper-hdro/badge.svg?branch=main&ts=1)](https://coveralls.io/github/OCHA-DAP/hdx-scraper-hdro?branch=main)

This script connects to the [HDRO API](https://hdrdata.org) and extracts country by country creating a dataset per country in HDX. It makes around 200 reads from HDRO and 200 read/writes (API calls) to HDX in total. It creates 2 temporary files per country of no more than 100Kb size. It is run every year. 


### Usage

    python run.py

For the script to run, you will need to have a file called .hdx_configuration.yaml in your home directory containing your HDX key eg.

    hdx_key: "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
    hdx_read_only: false
    hdx_site: prod
    
 You will also need to supply the universal .useragents.yaml file in your home directory as specified in the parameter *user_agent_config_yaml* passed to facade in run.py. The collector reads the key **hdx-scraper-hdro** as specified in the parameter *user_agent_lookup*.
 
 Alternatively, you can set up environment variables: USER_AGENT, HDX_KEY, HDX_SITE, TEMP_DIR, API_KEY 
