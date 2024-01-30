#!/usr/bin/python
"""
HDRO:
------------

Reads Hdro JSON and creates datasets.

"""

import logging
from slugify import slugify

from hdx.data.dataset import Dataset
from hdx.location.country import Country
from hdx.utilities.dateparse import parse_date_range
from hdx.utilities.dictandlist import dict_of_lists_add

logger = logging.getLogger(__name__)


class HDRO:
    hxltags = {
        "country_code": "#country+code",
        "country_name": "#country+name",
        "indicator_id": "#indicator+id",
        "indicator_name": "#indicator+name",
        "index_id": "#index+id",
        "index_name": "#index+name",
        "year": "#date+year",
        "value": "#indicator+value+num",
    }

    def __init__(self, configuration, retriever, folder):
        self.configuration = configuration
        self.retriever = retriever
        self.folder = folder
        self.country_data = {}
        self.aggregate_data = {}

    def get_country_data(self, countries_to_process):
        for country_iso3 in countries_to_process:
            base_url = self.configuration["base_url"]
            jsonresponse = self.retriever.download_json(f"{base_url}{country_iso3}")

            for row in jsonresponse:
                # split hyphenated strings into separate columns
                country = row["country"].split(' - ')
                indicator = row["indicator"].split(' - ')
                index = row["index"].split(' - ')

                obj = {
                    "country_code": country[0],
                    "country_name": country[1],
                    "indicator_id": indicator[0],
                    "indicator_name": indicator[1],
                    "index_id": index[0],
                    "index_name": index[1],
                    "value": row["value"],
                    "year": row["year"]
                }

                # save indicator and aggregate values to separate dicts
                if obj["indicator_id"].lower() == obj["index_id"].lower():
                    dict_of_lists_add(self.aggregate_data, obj["country_code"], obj)
                else:
                    dict_of_lists_add(self.country_data, obj["country_code"], obj)

        return [{"iso3": countryiso} for countryiso in sorted(self.country_data)]

    def generate_dataset(self, countryiso, quickcharts):
        countrydata = self.country_data[countryiso]
        countryaggdata = self.aggregate_data.get(countryiso)
        countryname = Country.get_country_name_from_iso3(countryiso)
        title = f"{countryname} - Human Development Indicators"
        logger.info(f"Creating dataset: {title}")
        name = f"HDRO data for {countryname}"
        slugified_name = slugify(name).lower()

        dataset = Dataset({
            "name": slugified_name,
            "title": title
        })
        dataset.set_maintainer("872427e4-7e9b-44d6-8c58-30d5052a00a2")
        dataset.set_organization("89ebe982-abe9-4748-9dde-cf04632757d6")
        dataset.set_expected_update_frequency("Every year")
        dataset.set_subnational(False)
        dataset.add_country_location(countryiso)
        tags = [
            "health",
            "education",
            "socioeconomic",
            "demographics",
            "development",
            "indicators",
            "hxl",
        ]
        dataset.add_tags(tags)

        def yearcol_function(row):
            result = dict()
            year = row["year"]
            if year:
                if len(year) == 9:
                    startyear = year[:4]
                    endyear = year[5:]
                    result["startdate"], _ = parse_date_range(startyear, date_format="%Y")
                    _, result["enddate"] = parse_date_range(endyear, date_format="%Y")
                else:
                    result["startdate"], result["enddate"] = parse_date_range(
                        year, date_format="%Y"
                    )
            return result

        filename = f"hdro_indicators_{countryiso.lower()}.csv"
        resource = {
            "name": f"Human Development Indicators for {countryname}",
            "description": "Human development data with HXL tags"
        }

        success, _ = dataset.generate_resource_from_iterator(
            list(countrydata[0].keys()),
            countrydata,
            self.hxltags,
            self.folder,
            filename,
            resource,
            date_function=yearcol_function,
            quickcharts=None
        )

        if success is False:
            logger.error(f"{countryname} has no data!")
            return None

        hasQC = False
        if countryaggdata:
            # check for indicators needed for quick charts
            hasQC = any(d['index_id'] == 'GDI' or d['index_id'] == 'GII' for d in countryaggdata)

            filenameagg = f"hdro_indicators_aggregates_{countryiso.lower()}.csv"
            resourceagg = {
                "name": f"Aggregated Human Development Indicators for {countryname}",
                "description": "Aggregated human development data with HXL tags"
            }
            success, _ = dataset.generate_resource_from_iterator(
                list(countryaggdata[0].keys()),
                countryaggdata,
                self.hxltags,
                self.folder,
                filenameagg,
                resourceagg,
                date_function=yearcol_function,
                quickcharts=quickcharts if hasQC else None
            )

        if success is False:
            logger.error(f"{countryname} has no aggregate data!")
            return None

        return dataset, hasQC

