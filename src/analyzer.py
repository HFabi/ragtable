import pandas as pd

from src.config import MetadataConfig


class Analyzer:
    """Analyze a dataframe"""

    @staticmethod
    def _column_sum_percentage(df, column_name, total) -> tuple[int, float]:
        """

        :param df:
        :param column_name:
        :param total:
        :return:
        """
        column_sum = df[column_name].sum()
        return column_sum, (column_sum / total) * 100

    def generate_metadata(self,
                          df: pd.DataFrame,
                          config: MetadataConfig,
                          run_name: str,
                          parameter: dict | None = None,
                          suffix: str | None = None
                          ) -> pd.DataFrame:
        """

        :param df:
        :param config:
        :param run_name:
        :param parameter:
        :param suffix:
        :return:
        """
        total = df.shape[0]

        # add run_name and row_count
        result_row = {'run': run_name, 'row_count': total}

        # add additional parameters if specified, e.g. llm, top_n, ...
        if parameter:
            result_row.update(parameter)

        # add discrete metrics
        for metric_name in config.discrete_metrics:
            # columns are prefixed to distinguish different runs
            # but in the result_row we want to have the name of the original/un-prefixed metric for comparison
            column_name = f"{metric_name}_{suffix}" if suffix else metric_name
            result_row[f'{metric_name}_mean'] = df.aggregate({column_name: ['mean']}).values[0][0]
            result_row[f'{metric_name}_min'] = df.aggregate({column_name: ['min']}).values[0][0]
            result_row[f'{metric_name}_max'] = df.aggregate({column_name: ['max']}).values[0][0]

        # add rate metrics
        for metric_name in config.rate_metrics:
            column_name = f"{metric_name}_{suffix}" if suffix else metric_name
            col_sum, percent = self._column_sum_percentage(df, column_name, total)
            result_row[f'{metric_name}_rate'] = percent
            result_row[f'{metric_name}_count'] = col_sum

        # add dynamic metrics
        for metric_name, aggregation in config.custom_metrics:
            column_name = f"{metric_name}_{suffix}" if suffix else metric_name
            result_row[f'{metric_name}_{aggregation}'] = df.aggregate({column_name: [aggregation]}).values[0][0]

        # wrap row into a dataframe
        df_result = pd.DataFrame([result_row])
        #df_result.loc[len(df_result)] = result_row
        return df_result
