#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os.path import join, expanduser

from hdx.api.configuration import Configuration
from hdx.data.hdxobject import HDXError
from hdx.location.country import Country
from hdx.data.resource import Resource
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir, progress_storing_tempdir

from hdro import generate_dataset, get_country_data

from hdx.facades.simple import facade

logger = logging.getLogger(__name__)


# HDX only
lookup = "hdx-scraper-hdro"


def main():
    """Generate dataset and create it in HDX"""
    with Download() as downloader:
        base_url = Configuration.read()["base_url"]
        country_dict, aggregate_dict = get_country_data(base_url, downloader)
        countries = [
            {"iso3": countryiso} for countryiso in sorted(country_dict.keys())
        ]
        logger.info(f"Number of countries to upload: {len(countries)}")

        datasets_to_update = []
        for info, country in progress_storing_tempdir("hdro", countries, "iso3"):
            countryiso = country["iso3"]
            countryname = Country.get_country_name_from_iso3(countryiso)

            # create indicator datsaet per country
            countrydata = country_dict[countryiso]
            filename = f"hdro_indicators_{countryiso.lower()}.csv"
            resource = Resource({
                "name": f"Human Development Indicators for {countryname}",
                "description": "Human development data with HXL tags"
            })
            dataset = generate_dataset(
                info["folder"], countryiso, countrydata, filename, resource
            )
            if dataset:
                datasets_to_update.append([dataset, info["batch"]])
                dataset.update_from_yaml()
                dataset.create_in_hdx(hxl_update=False, updated_by_script="HDX Scraper: HDRO", batch=info["batch"])

            # create aggregated index dataset per country
            countryaggdata = aggregate_dict[countryiso]
            filenameagg = f"hdro_indicators_aggregates_{countryiso.lower()}.csv"
            resourceagg = Resource({
                "name": f"Aggregated Human Development Indicators for {countryname}",
                "description": "Aggregated human development data with HXL tags"
            })
            datasetagg = generate_dataset(
                info["folder"], countryiso, countryaggdata, filenameagg, resourceagg
            )
            if datasetagg:
                datasets_to_update.append([datasetagg, info["batch"]])
                datasetagg.update_from_yaml()
                datasetagg.create_in_hdx(hxl_update=False, updated_by_script="HDX Scraper: HDRO", batch=info["batch"])


        # for dataset, batch in datasets_to_update:
        #     dataset.update_from_yaml()
        #     dataset.create_in_hdx(hxl_update=False, updated_by_script="HDX Scraper: HDRO", batch=batch)



if __name__ == "__main__":
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yml"),
        user_agent_lookup=lookup,
        project_config_yaml=join("config", "project_configuration.yml")
    )
    # HDX: Remember to create .hdx_configuration.yml on your server eg. the ScraperWiki box!
    # HDX: Use facade below creating or adding to .useragents.yml a key (hdx-scraper-hdro) with a Dict as
    # a value, the Dict containing user agent entries.
    # HDX: It is best to use the HDX Data Team bot"s key (https://data.humdata.org/user/luiscape) rather than your own.
    # HDX: That file should have a user_agent parameter and an additional one identifying the scraper as internal to HDX.
    # facade(main, user_agent_config_yaml=join(expanduser("~"), ".useragents.yml"), user_agent_lookup=lookup, project_config_yaml=join("config", "project_configuration.yml"))


