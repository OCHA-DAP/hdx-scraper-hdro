#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import argparse
import logging
from os import getenv
from os.path import join, expanduser

from hdx.api.configuration import Configuration
from hdx.facades.keyword_arguments import facade
from hdx.location.country import Country
from hdx.utilities.downloader import Download
from hdx.utilities.path import progress_storing_folder, wheretostart_tempdir_batch
from hdx.utilities.retriever import Retrieve

from hdro import HDRO

logger = logging.getLogger(__name__)

lookup = "hdx-scraper-hdro"
updated_by_script = "HDX Scraper: HDRO"


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-ak", "--api_key", default=None, help="Key for accessing HDRO API",
    )
    parser.add_argument(
        "-s", "--save", default=False, action="store_true", help="Save data for testing",
    )
    parser.add_argument(
        "-usv", "--use-saved", default=False, action="store_true", help="Use saved data",
    )
    args = parser.parse_args()
    return args


def main(api_key: str, save: bool = False, use_saved: bool = False, **ignore) -> None:
    """Generate dataset and create it in HDX"""
    with wheretostart_tempdir_batch(lookup) as info:
        folder = info["folder"]
        with Download() as downloader:
            retriever = Retrieve(
                downloader, folder, "saved_data", folder, save, use_saved
            )
            folder = info["folder"]
            batch = info["batch"]
            configuration = Configuration.read()
            qc_indicators = configuration["qc_indicators"]
            hdro = HDRO(configuration, retriever, folder)
            countries_to_process = Country.countriesdata()["countries"].keys()
            countries = hdro.get_country_data(countries_to_process, api_key)
            logger.info(f"Number of countries to upload: {len(countries)}")

            for _, nextdict in progress_storing_folder(info, countries, "iso3"):
                countryiso = nextdict["iso3"]
                quickcharts = {
                    "hashtag": "#index+id",
                    "values": [x["code"] for x in qc_indicators],
                    "cutdown": 2,
                    "cutdownhashtags": ["#index+id", "#date+year", "#indicator+value+num"],
                }

                dataset, showcase, bites_disabled = hdro.generate_dataset(countryiso, quickcharts)

                if dataset:
                    dataset.update_from_yaml()
                    dataset.generate_quickcharts(
                        -1, bites_disabled=bites_disabled, indicators=qc_indicators
                    )
                    dataset.create_in_hdx(
                        remove_additional_resources=True,
                        hxl_update=False,
                        updated_by_script=updated_by_script,
                        batch=batch,
                    )
                    showcase.create_in_hdx()
                    showcase.add_dataset(dataset)


if __name__ == "__main__":
    args = parse_args()
    api_key = args.api_key
    if api_key is None:
        api_key = getenv("API_KEY")
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=lookup,
        project_config_yaml=join("config", "project_configuration.yaml"),
        api_key=api_key,
        save=args.save,
        use_saved=args.use_saved,
    )
