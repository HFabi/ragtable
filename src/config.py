from dataclasses import dataclass

from src.constants import ConfigYML


@dataclass
class MetadataConfig:
    discrete_metrics: [str]
    rate_metrics: [str]
    custom_metrics: [str]


class Config:

    def __init__(self, data: dict):
        self._config_dict = data
        self.use_case_name = self._config_dict.get(ConfigYML.USE_CASE_NAME)
        self.metadata: MetadataConfig = self._get_metadata()

    def get_transform(self, flavour: str = ConfigYML.FLAVOR_DEFAULT) -> dict:
        # create config dict for flavour
        config_transform = self._config_dict.get(ConfigYML.TRANSFORM)
        config_flavour = config_transform.get(ConfigYML.FLAVOR_DEFAULT)
        if flavour != ConfigYML.FLAVOR_DEFAULT:
            config_flavour = config_flavour.update(config_transform.get(flavour))
        return config_flavour

    def _get_metadata(self) -> MetadataConfig:
        metadata = self._config_dict.get(ConfigYML.METADATA)
        discrete_metrics = metadata.get(ConfigYML.METADATA_METRICS_DISCRETE)
        rate_metrics = metadata.get(ConfigYML.METADATA_METRICS_RATE)
        custom_metrics = metadata.get(ConfigYML.METADATA_METRICS_CUSTOM)
        return MetadataConfig(
            discrete_metrics=discrete_metrics if discrete_metrics else [],
            rate_metrics=rate_metrics if rate_metrics else [],
            custom_metrics=custom_metrics if custom_metrics else [],
        )
