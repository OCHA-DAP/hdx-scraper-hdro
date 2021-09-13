#!/usr/bin/python
"""
Unit tests for HDRO.

"""
from os.path import join

import pytest
from hdx.data.vocabulary import Vocabulary
from hdx.hdx_configuration import Configuration
from hdx.hdx_locations import Locations
from hdx.location.country import Country
from hdx.utilities.compare import assert_files_same
from hdx.utilities.path import temp_dir

from hdro import generate_dataset_and_showcase, get_countriesdata

hdro_data = [
    {
        "indicator_id": 137506,
        "indicator_name": "Human Development Index (HDI)",
        "country_code": "AFG",
        "country_name": "Afghanistan",
        "year": "2017",
        "value": "0.497695114",
    },
    {
        "indicator_id": 137506,
        "indicator_name": "Human Development Index (HDI)",
        "country_code": "AFG",
        "country_name": "Afghanistan",
        "year": "2016",
        "value": "0.494239738",
    },
    {
        "indicator_id": 23906,
        "indicator_name": "Population with at least some secondary education, female (% ages 25 and older)",
        "country_code": "AFG",
        "country_name": "Afghanistan",
        "year": "2017",
        "value": "11.36",
    },
    {
        "indicator_id": 138806,
        "indicator_name": "Inequality-adjusted HDI (IHDI)",
        "country_code": "AFG",
        "country_name": "Afghanistan",
        "year": "2017",
        "value": "0.350428496",
    },
    {
        "indicator_id": 38406,
        "indicator_name": "Multidimensional poverty index (MPI)",
        "country_code": "AFG",
        "country_name": "Afghanistan",
        "year": "2008-2019",
        "value": 0.27172124,
    },
]


class Response:
    @staticmethod
    def json():
        pass


class Download:
    @staticmethod
    def download(url):
        response = Response()
        if url == "http://lala/":

            def fn():
                return hdro_data

            response.json = fn
        return response


class TestHDRO:
    @pytest.fixture(scope="function")
    def configuration(self):
        Configuration._create(
            hdx_read_only=True,
            user_agent="test",
            project_config_yaml=join("tests", "config", "project_configuration.yml"),
        )
        Locations.set_validlocations([{"name": "afg", "title": "Afghanistan"}])
        Country.countriesdata(use_live=False)
        Vocabulary._tags_dict = True
        Vocabulary._approved_vocabulary = {
            "tags": [
                {"name": "hxl"},
                {"name": "indicators"},
                {"name": "health"},
                {"name": "education"},
                {"name": "socioeconomic"},
                {"name": "demographics"},
                {"name": "development"},
            ],
            "id": "4e61d464-4943-4e97-973a-84673c1aaa87",
            "name": "approved",
        }
        return Configuration.read()

    @pytest.fixture(scope="function")
    def downloader(self):
        return Download()

    def test_get_countriesdata(self, downloader):
        countriesdata = get_countriesdata("http://lala/", downloader)
        assert countriesdata == {"AFG": hdro_data}

    def test_generate_dataset_and_showcase(self, configuration):
        with temp_dir("HDRO") as folder:
            qc_indicators = configuration["qc_indicators"]
            dataset, showcase, bites_disabled = generate_dataset_and_showcase(
                folder, "AFG", hdro_data, qc_indicators
            )
            assert dataset == {
                "name": "hdro-data-for-afghanistan",
                "title": "Afghanistan - Human Development Indicators",
                "maintainer": "872427e4-7e9b-44d6-8c58-30d5052a00a2",
                "owner_org": "89ebe982-abe9-4748-9dde-cf04632757d6",
                "data_update_frequency": "365",
                "subnational": "0",
                "groups": [{"name": "afg"}],
                "tags": [
                    {
                        "name": "health",
                        "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                    },
                    {
                        "name": "education",
                        "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                    },
                    {
                        "name": "socioeconomic",
                        "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                    },
                    {
                        "name": "demographics",
                        "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                    },
                    {
                        "name": "development",
                        "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                    },
                    {
                        "name": "indicators",
                        "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                    },
                    {
                        "name": "hxl",
                        "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                    },
                ],
                "dataset_date": "[2008-01-01T00:00:00 TO 2019-12-31T00:00:00]",
            }
            resources = dataset.get_resources()
            assert resources == [
                {
                    "name": "Human Development Indicators for Afghanistan",
                    "description": "Human development data with HXL tags",
                    "format": "csv",
                    "resource_type": "file.upload",
                    "url_type": "upload",
                },
                {
                    "name": "QuickCharts-Human Development Indicators for Afghanistan",
                    "description": "Cut down data for QuickCharts",
                    "format": "csv",
                    "resource_type": "file.upload",
                    "url_type": "upload",
                },
            ]

            assert showcase == {
                "name": "hdro-data-for-afghanistan-showcase",
                "title": "Indicators for Afghanistan",
                "notes": "Human Development indicators for Afghanistan",
                "url": "http://hdr.undp.org/en/countries/profiles/AFG",
                "image_url": "https://s1.stabroeknews.com/images/2019/12/undp.jpg",
                "tags": [
                    {
                        "name": "health",
                        "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                    },
                    {
                        "name": "education",
                        "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                    },
                    {
                        "name": "socioeconomic",
                        "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                    },
                    {
                        "name": "demographics",
                        "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                    },
                    {
                        "name": "development",
                        "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                    },
                    {
                        "name": "indicators",
                        "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                    },
                    {
                        "name": "hxl",
                        "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                    },
                ],
            }
            assert bites_disabled == [False, False, True]
            file = "hdro_indicators_AFG.csv"
            assert_files_same(join("tests", "fixtures", file), join(folder, file))
            file = f"qc_{file}"
            assert_files_same(join("tests", "fixtures", file), join(folder, file))
