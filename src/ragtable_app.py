from typing import Literal

import pandas as pd

from src.analyzer import Analyzer
from src.config import Config
from src.models.ragtable import RagTable
from src.models.run import Run
from src.store_handler import StoreHandler
from src.transformer import Transformer
from usecases.sample.transformer_sample import SampleTransformer


class RagTableApp:

    def __init__(self, analyzer: Analyzer, store_handler: StoreHandler, transformer: Transformer, config: Config):
        self.analyzer = analyzer
        self.store_handler = store_handler
        self.transformer = transformer
        self.config = config

    def preprocess_run(
            self,
            run_name: str,
            flavour: str = 'default',
            parameter: dict | None = None
    ) -> None:
        """
        Reads in raw data and creates a new run (transform + evaluate)

        :param run_name: run name, needs to be unique
        :param flavour:
        :param parameter:
        :return: None
        """
        # extract from source
        df_source = self.store_handler.load_source_data(run_name)

        # transform and metadata
        df_data = self.transformer.apply(df_source, self.config.get_transform(flavour))
        df_metadata = self.analyzer.generate_metadata(df_data, self.config.metadata, run_name, parameter, None)

        # load into storage
        run = Run(run_name, df_data, df_metadata)
        self.store_handler.store_run(run)

    def create_ragtable(self,
                        from_runs: dict[str, str],
                        merge_runs_on: list[str],
                        select: list[str],
                        where: str | None,
                        merge_type: Literal["left", "right", "inner", "outer", "cross"] = 'inner',
                        ):
        # load
        runs: [Run] = self.store_handler.load_runs(from_runs)

        # add common columns
        df_all: pd.DataFrame = runs[0][[merge_runs_on]]
        # add columns of all the runs
        for run in runs:
            df = run.data.add_suffix(run.name)
            df_all = pd.merge(df_all, df, on=merge_runs_on, how=merge_type)

        return self._subtable(from_runs, df_all, select, where)

    def _subtable(self,
                  run_mapping: dict[str, str],
                  df_data: pd.DataFrame,
                  select: list[str],
                  query: str | None,
                  ):
        # query
        if query:
            df_data = df_data.query(query)

        # metadata
        df_metadata = pd.DataFrame()
        for suffix in run_mapping.values():
            fm = analyzer.generate_metadata(df_data, self.config.metadata, suffix)
            df_metadata = pd.concat([metadata_all, fm])

        # filter
        df_data = df_data[select]
        return RagTable(run_mapping, df_data, df_metadata)

    def subtable(self,
                 t: RagTable,
                 select: list[str],
                 where: str,
                 ):
        return self.subtable(t.run_mapping, t.data, select, where)


if __name__ == '__main__':
    my_analyzer = Analyzer()
    my_store_handler = StoreHandler('sample')
    my_config = my_store_handler.load_config('config_sample.yml')
    my_transformer = SampleTransformer()

    ragtable_app = RagTableApp(my_analyzer, my_store_handler, my_transformer, my_config)

    ragtable_app.preprocess_run('uat_abc', 'default', {'llm': '4o-mini'})

    t1 = ragtable_app.create_ragtable(
        from_runs={'sample_isd': 'a', 'sample_dsd': 'b'},
        merge_runs_on=['question', 'answer', 'url'],
        select=['question', 'answer', 'llm_answer', 'a_groundedness'],
        where="a_recall > 0 and d_name == 1",
    )

    t2 = ragtable_app.subtable(
        t1,
        select=['question', 'z_answer', 'llm_answer', 'groundedness'],
        where='a < b',
    )
