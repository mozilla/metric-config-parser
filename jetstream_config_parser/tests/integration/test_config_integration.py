from jetstream_config_parser.config import ConfigCollection


class TestConfigIntegration:
    def test_configs_from_repo(self):
        config_collection = ConfigCollection.from_github_repo()
        assert config_collection is not None
        assert config_collection.get_platform_defaults("firefox_desktop") is not None
        assert (
            config_collection.spec_for_outcome(
                config_collection.outcomes[0].slug, config_collection.outcomes[0].platform
            )
            is not None
        )
        assert (
            config_collection.get_data_source_definition("clients_daily", "firefox_desktop")
            is not None
        )
        assert config_collection.get_metric_definition("baseline_ping_count", "fenix") is not None
        assert config_collection.get_metric_definition("not_exist", "firefox_desktop") is None
        assert (
            config_collection.get_segment_definition("regular_users_v3", "firefox_desktop")
            is not None
        )
