#!/usr/bin/python
"""
HDRO:
-----

Reads HDRO's API and creates datasets.

"""
# https://data.humdata.org/organization/undp-human-development-reports-office
import logging

from hdx.data.dataset import Dataset
from hdx.data.showcase import Showcase
from hdx.location.country import Country
from hdx.utilities.dateparse import parse_date_range
from hdx.utilities.dictandlist import dict_of_lists_add
from slugify import slugify

logger = logging.getLogger(__name__)

hxltags = {
    "indicator_id": "#indicator+code",
    "indicator_name": "#indicator+name",
    "country_code": "#country+code",
    "country_name": "#country+name",
    "year": "#date+year",
    "value": "#indicator+value+num",
}


def get_countriesdata(hdro_url, downloader):
    response = downloader.download(hdro_url)
    countriesdata = dict()
    for row in response.json():
        countryiso = row["country_code"]
        dict_of_lists_add(countriesdata, countryiso, row)
    return countriesdata


def generate_dataset_and_showcase(folder, countryiso, countrydata, qc_indicators):
    countryname = Country.get_country_name_from_iso3(countryiso)
    title = f"{countryname} - Human Development Indicators"
    slugified_name = slugify(f"HDRO data for {countryname}").lower()
    logger.info(f"Creating dataset: {title}")
    dataset = Dataset({"name": slugified_name, "title": title})
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

    filename = f"hdro_indicators_{countryiso}.csv"
    resourcedata = {
        "name": f"Human Development Indicators for {countryname}",
        "description": "Human development data with HXL tags",
    }
    quickcharts = {
        "hashtag": "#indicator+code",
        "values": [x["code"] for x in qc_indicators],
        "cutdown": 2,
        "cutdownhashtags": ["#indicator+code", "#date+year", "#indicator+value+num"],
    }

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

    success, results = dataset.generate_resource_from_iterator(
        countrydata[0].keys(),
        countrydata,
        hxltags,
        folder,
        filename,
        resourcedata,
        date_function=yearcol_function,
        quickcharts=quickcharts,
    )
    if success is False:
        logger.error(f"{countryname} has no data!")
        return None, None, None

    showcase = Showcase(
        {
            "name": f"{slugified_name}-showcase",
            "title": f"Indicators for {countryname}",
            "notes": f"Human Development indicators for {countryname}",
            "url": f"http://hdr.undp.org/en/countries/profiles/{countryiso}",
            "image_url": "https://s1.stabroeknews.com/images/2019/12/undp.jpg",
        }
    )
    showcase.add_tags(tags)

    return dataset, showcase, results["bites_disabled"]
