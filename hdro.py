#!/usr/bin/python
# -*- coding: utf-8 -*-
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
from hdx.utilities.dictandlist import dict_of_lists_add
from slugify import slugify

logger = logging.getLogger(__name__)

hxltags = {'indicator_id': '#indicator+code', 'indicator_name': '#indicator+name', 'country_code': '#country+code', 'country_name': '#country+name', 'year': '#date+year', 'value': '#indicator+value+num'}


def get_countriesdata(hdro_url, downloader):
    response = downloader.download(hdro_url)
    countriesdata = dict()
    for row in response.json():
        countryiso = row['country_code']
        dict_of_lists_add(countriesdata, countryiso, row)
    return countriesdata


def generate_dataset_and_showcase(folder, countryiso, countrydata, qc_indicators):
    countryname = Country.get_country_name_from_iso3(countryiso)
    title = '%s - Human Development Indicators' % countryname
    slugified_name = slugify('HDRO data for %s' % countryname).lower()
    logger.info('Creating dataset: %s' % title)
    dataset = Dataset({
        'name': slugified_name,
        'title': title
    })
    dataset.set_maintainer('872427e4-7e9b-44d6-8c58-30d5052a00a2')
    dataset.set_organization('89ebe982-abe9-4748-9dde-cf04632757d6')
    dataset.set_expected_update_frequency('Every year')
    dataset.set_subnational(False)
    dataset.add_country_location(countryiso)
    tags = ['health', 'education', 'socioeconomic', 'demographics', 'development', 'indicators', 'hxl']
    dataset.add_tags(tags)

    filename = 'hdro_indicators_%s.csv' % countryiso
    resourcedata = {
        'name': 'Human Development Indicators for %s' % countryname,
        'description': 'Human development data with HXL tags'
    }
    quickcharts = {'hashtag': '#indicator+code', 'values': [x['code'] for x in qc_indicators],
                   'cutdown': 2, 'cutdownhashtags': ['#indicator+code', '#date+year', '#indicator+value+num']}
    success, results = dataset.generate_resource_from_iterator(
        countrydata[0].keys(), countrydata, hxltags, folder, filename, resourcedata,
        yearcol='year', quickcharts=quickcharts)
    if success is False:
        logger.error('%s has no data!' % countryname)
        return None, None, None

    showcase = Showcase({
        'name': '%s-showcase' % slugified_name,
        'title': 'Indicators for %s' % countryname,
        'notes': 'Human Development indicators for %s' % countryname,
        'url': 'http://hdr.undp.org/en/countries/profiles/%s' % countryiso,
        'image_url': 'https://s1.stabroeknews.com/images/2019/12/undp.jpg'
    })
    showcase.add_tags(tags)

    return dataset, showcase, results['bites_disabled']
