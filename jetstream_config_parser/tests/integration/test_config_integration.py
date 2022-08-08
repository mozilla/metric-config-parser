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
