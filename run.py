#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os.path import join, expanduser

from hdx.hdx_configuration import Configuration
from hdx.utilities.downloader import Download
from hdx.utilities.path import progress_storing_tempdir

from hdro import get_countriesdata, generate_dataset_and_showcase

from hdx.facades.simple import facade

logger = logging.getLogger(__name__)

lookup = 'hdx-scraper-hdro'


def main():
    """Generate dataset and create it in HDX"""

    hdro_url = Configuration.read()['hdro_url']
    with Download() as downloader:
        countriesdata = get_countriesdata(hdro_url, downloader)
        countries = [{'iso3': countryiso} for countryiso in sorted(countriesdata.keys())]
        logger.info('Number of countries to upload: %d' % len(countries))
        for folder, country in progress_storing_tempdir('HDRO',  countries, 'iso3'):
            countryiso = country['iso3']
            countrydata = countriesdata[countryiso]
            dataset, showcase, bites_disabled = generate_dataset_and_showcase(folder, countryiso, countrydata)
            if dataset:
                dataset.update_from_yaml()
                dataset.generate_resource_view(-1, bites_disabled=bites_disabled)
                dataset.create_in_hdx(remove_additional_resources=True, hxl_update=False, updated_by_script='HDX Scraper: HDRO')
                showcase.create_in_hdx()
                showcase.add_dataset(dataset)


if __name__ == '__main__':
    facade(main, user_agent_config_yaml=join(expanduser('~'), '.useragents.yml'), user_agent_lookup=lookup, project_config_yaml=join('config', 'project_configuration.yml'))

