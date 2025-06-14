import glob
import os.path

import pandas as pd
import yaml

from src.config import Config
from src.constants import App
from src.models.run import Run


class StoreHandler:
    """handles a project store"""

    def __init__(self, project_name: str):
        self.project_name = project_name

    def load_config(self, config_file_path: str) -> Config:
        """

        :param config_file_path:
        :return:
        """
        with open(config_file_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

        return Config(config)

    def load_source_data(self, run_name: str) -> pd.DataFrame:
        """

        :param run_name:
        :return:
        """
        source_abs_path = os.path.join(App.DATA_DIR_PATH, self.project_name, App.SOURCE_DIR_NAME)
        print(os.listdir(source_abs_path))
        print(glob.glob(f'{source_abs_path}/**/{run_name}.csv', recursive=True))

        run_file_path_list = glob.glob(f'{source_abs_path}/**/{run_name}.csv', recursive=True)
        if len(run_file_path_list) != 1:
            raise Exception('File not found')

        df = pd.read_csv(run_file_path_list[0])
        return df

    def load_runs(self, run_target_names: dict[str, str]) -> [Run]:
        runs: [Run] = []
        for run_name, target_name in run_target_names:
            runs.append(self.load_run(run_name, target_name))
        return runs

    def load_run(self, run_name: str, target_name: str | None) -> Run:
        file_path = os.path.join(App.DATA_DIR_PATH, self.project_name, App.RUN_DIR_NAME, run_name)
        df = pd.read_csv(f'{file_path}.csv')
        df_meta = pd.read_csv(f'{file_path}_meta.csv')
        name = target_name if target_name else run_name
        return Run(name=name, data=df, metadata=df_meta)

    def store_run(self, run: Run) -> None:
        output_dir_path = os.path.join(App.DATA_DIR_PATH, self.project_name, App.RUN_DIR_NAME)
        os.makedirs(output_dir_path, exist_ok=True)

        file_path = os.path.join(output_dir_path, f"{run.name}.csv")
        file_path_meta = os.path.join(output_dir_path, f"{run.name}_meta.csv")
        run.data.to_csv(file_path, index=False)
        run.metadata.to_csv(file_path_meta, index=False)

    #def load_ragtable(self, name: str) -> RagTable:
    #    output_dir_path = os.path.join(App.DATA_DIR_PATH, self.project_name, App.RUN_SET_DIR_NAME)
    #    os.makedirs(output_dir_path, exist_ok=True)

    #        file_path = os.path.join(output_dir_path, f"{name}.csv")
    #    file_path_meta = os.path.join(output_dir_path, f"{name}_meta.csv")
    #    run.data.to_csv(file_path, index=False)
    #    run.data.to_csv(file_path_meta, index=False)

    #def store_ragtable(self, runset: RagTable) -> None:
    #    output_dir_path = os.path.join(App.DATA_DIR_PATH, self.project_name, App.RUN_SET_DIR_NAME)
    #    os.makedirs(output_dir_path, exist_ok=True)

    #    file_path = os.path.join(output_dir_path, f"{runset.name}.csv")
    #    file_path_meta = os.path.join(output_dir_path, f"{runset.name}_meta.csv")
    #    run.data.to_csv(file_path, index=False)
    #    run.data.to_csv(file_path_meta, index=False)


# with open(f'{file_path}.json') as f:
#    metadata = json.load(f)