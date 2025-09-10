import os
from os.path import join

from hdx.utilities.compare import assert_files_same
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve

from hdx.scraper.hdro.pipeline import Pipeline


class TestPipeline:
    def test_pipeline(self, configuration, fixtures_dir, input_dir, config_dir):
        HDRO_API_KEY = os.getenv("HDRO_API_KEY")

        with temp_dir(
            "TestHdro",
            delete_on_success=True,
            delete_on_failure=False,
        ) as tempdir:
            with Download(user_agent="test") as downloader:
                retriever = Retrieve(
                    downloader=downloader,
                    fallback_dir=tempdir,
                    saved_dir=input_dir,
                    temp_dir=tempdir,
                    save=False,
                    use_saved=True,
                )
                pipeline = Pipeline(configuration, retriever, tempdir, HDRO_API_KEY)
                countries_to_process = {"AFG": None}.keys()
                countries = pipeline.get_country_data(countries_to_process)

                countryiso = countries[0]["iso3"]

                dataset, showcase = pipeline.generate_dataset(countryiso)

                dataset.update_from_yaml(
                    path=join(config_dir, "hdx_dataset_static.yaml")
                )
                assert dataset == {
                    "caveats": "",
                    "data_update_frequency": 180,
                    "dataset_date": "[1990-01-01T00:00:00 TO 2022-12-31T23:59:59]",
                    "dataset_source": "HDRO",
                    "groups": [{"name": "afg"}],
                    "license_id": "cc-by-igo",
                    "maintainer": "872427e4-7e9b-44d6-8c58-30d5052a00a2",
                    "methodology": "Registry",
                    "name": "hdro-data-for-afghanistan",
                    "notes": "The aim of the Human Development Report is to stimulate global, "
                    "regional and national policy-relevant discussions on issues "
                    "pertinent to human development. Accordingly, the data in the Report "
                    "require the highest standards of data quality, consistency, "
                    "international comparability and transparency. The Human Development "
                    "Report Office (HDRO) fully subscribes to the Principles governing "
                    "international statistical activities.\n"
                    "\n"
                    "The HDI was created to emphasize that people and their capabilities "
                    "should be the ultimate criteria for assessing the development of a "
                    "country, not economic growth alone. The HDI can also be used to "
                    "question national policy choices, asking how two countries with the "
                    "same level of GNI per capita can end up with different human "
                    "development outcomes. These contrasts can stimulate debate about "
                    "government policy priorities.\n"
                    "The Human Development Index (HDI) is a summary measure of average "
                    "achievement in key dimensions of human development: a long and "
                    "healthy life, being knowledgeable and have a decent standard of "
                    "living. The HDI is the geometric mean of normalized indices for "
                    "each of the three dimensions.\n"
                    "\n"
                    "The 2019 Global Multidimensional Poverty Index (MPI) data shed "
                    "light on the number of people experiencing poverty at regional, "
                    "national and subnational levels, and reveal inequalities across "
                    "countries and among the poor themselves.Jointly developed by the "
                    "United Nations Development Programme (UNDP) and the Oxford Poverty "
                    "and Human Development Initiative (OPHI) at the University of "
                    "Oxford, the 2019 global MPI offers data for 101 countries, covering "
                    "76 percent of the global population.\n"
                    "The MPI provides a comprehensive and in-depth picture of global "
                    "poverty – in all its dimensions – and monitors progress towards "
                    "Sustainable Development Goal (SDG) 1 – to end poverty in all its "
                    "forms. It also provides policymakers with the data to respond to "
                    "the call of Target 1.2, which is to ‘reduce at least by half the "
                    "proportion of men, women, and children of all ages living in "
                    "poverty in all its dimensions according to national definition'.",
                    "owner_org": "89ebe982-abe9-4748-9dde-cf04632757d6",
                    "package_creator": "HDX Data Systems Team",
                    "private": False,
                    "subnational": "0",
                    "tags": [
                        {
                            "name": "health",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "education",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "gender",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "socioeconomics",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "demographics",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "development",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "indicators",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "title": "Afghanistan - Human Development Indicators",
                }

                resources = dataset.get_resources()
                assert resources == [
                    {
                        "name": "Human Development Indicators for Afghanistan",
                        "description": "Human development data with HXL tags",
                        "format": "csv",
                    },
                    {
                        "name": "Aggregated Human Development Indicators for Afghanistan",
                        "description": "Aggregated human development data with HXL tags",
                        "format": "csv",
                    },
                ]
                for resource in resources:
                    if (
                        resource["name"]
                        == "Human Development Indicators for Afghanistan"
                    ):
                        filename = "hdro_indicators_afg.csv"
                    else:
                        filename = "hdro_indicators_aggregates_afg.csv"
                    actual = join(tempdir, filename)
                    expected = join(input_dir, filename)
                    assert_files_same(actual, expected)
