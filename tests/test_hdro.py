#!/usr/bin/python
"""
Unit tests for HDRO

"""
from os.path import join

import pytest
from hdx.api.configuration import Configuration
from hdx.api.locations import Locations
from hdx.data.vocabulary import Vocabulary
from hdx.location.country import Country
from hdx.utilities.compare import assert_files_same
from hdx.utilities.dateparse import parse_date
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve
from hdx.utilities.useragent import UserAgent
from hdro import HDRO


class TestHDRO:
    dataset = {
        "data_update_frequency": "365",
        "dataset_date": "[1990-01-01T00:00:00 TO 2021-12-31T23:59:59]",
        "groups": [{"name": "afg"}],
        "maintainer": "872427e4-7e9b-44d6-8c58-30d5052a00a2",
        "name": "hdro-data-for-afghanistan",
        "owner_org": "89ebe982-abe9-4748-9dde-cf04632757d6",
        "subnational": "0",
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
                "name": "gender",
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
        "title": "Afghanistan - Human Development Indicators",
    }
    resources = [
        {
            "name": "Human Development Indicators for Afghanistan",
            "description": "Human development data with HXL tags",
            "format": "csv",
            "resource_type": "file.upload",
            "url_type": "upload",
        },
        {
            "name": "Aggregated Human Development Indicators for Afghanistan",
            "description": "Aggregated human development data with HXL tags",
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

    @pytest.fixture(scope="function")
    def fixtures(self):
        return join("tests", "fixtures")

    @pytest.fixture(scope="function")
    def configuration(self):
        Configuration._create(
            hdx_read_only=True,
            user_agent="test",
            project_config_yaml=join("config", "project_configuration.yml"),
        )
        UserAgent.set_global("test")
        Locations.set_validlocations([{"name": "afg", "title": "Afghanistan"}])
        Country.countriesdata(use_live=False)
        tags = (
            "hxl",
            "indicators",
            "health",
            "education",
            "socioeconomic",
            "demographics",
            "gender",
            "development",
        )
        Vocabulary._tags_dict = {tag: {"Action to Take": "ok"} for tag in tags}
        tags = [{"name": tag} for tag in tags]
        Vocabulary._approved_vocabulary = {
            "tags": tags,
            "id": "4e61d464-4943-4e97-973a-84673c1aaa87",
            "name": "approved",
        }
        return Configuration.read()

    def test_generate_dataset(self, configuration, fixtures):
        with temp_dir(
            "test_hdro", delete_on_success=True, delete_on_failure=False
        ) as folder:
            with Download() as downloader:
                retriever = Retrieve(downloader, folder, fixtures, folder, False, True)
                qc_indicators = configuration["qc_indicators"]
                quickcharts = {
                    "hashtag": "#index+id",
                    "values": [x["code"] for x in qc_indicators],
                    "cutdown": 2,
                    "cutdownhashtags": ["#index+id", "#date+year", "#indicator+value+num"],
                }

                # indicator dataset test
                hdro = HDRO(configuration, retriever, folder)
                countries = hdro.get_country_data(["AFG"])
                assert countries == [{"iso3": "AFG"}]

                dataset, bites_disabled = hdro.generate_dataset("AFG", quickcharts)
                assert dataset == self.dataset
                resources = dataset.get_resources()
                assert resources[0] == self.resources[0]
                file = "hdro_indicators_afg.csv"
                assert_files_same(join("tests", "fixtures", file), join(folder, file))

                assert resources[1] == self.resources[1]
                file2 = "hdro_indicators_aggregates_afg.csv"
                assert_files_same(join("tests", "fixtures", file2), join(folder, file2))