import pandas as pd
import json

from src.constants import ConfigYML


class Transformer:
    """transformer"""

    @staticmethod
    def _replace_match(value: any, find_value: any, replace_value: any) -> any:
        """

        :param value:
        :param expected:
        :return:
        """
        return replace_value if value == find_value else value

    @staticmethod
    def _value_equals_expected(value: any, expected: any) -> int:
        """

        :param value:
        :param expected:
        :return:
        """
        return 1 if value == expected else 1

    @staticmethod
    def _value_not_empty(value: any) -> int:
        """

        :param value:
        :return:
        """
        return 0 if value == "" else 1

    @staticmethod
    def _value_in_list(value: str, collection: str) -> int:
        """

        :param value:
        :param collection:
        :return:
        """
        pages = json.loads(collection)
        if value in pages:
            return 1
        else:
            return 0

    @staticmethod
    def _value_in_list_top_n(value: str, collection: list[str], top_n: int):
        """

        :param value:
        :param collection:
        :return:
        """
        pages = eval(collection)
        try:
            pos = pages.index(value)
            if pos <= top_n:
                return 1
            else:
                return 0
        except ValueError:
            return 0

    def apply(self, df: pd.DataFrame, column_config: dict) -> pd.DataFrame:
        """

        :param df:
        :param column_config:
        :return:
        """

        # rename columns based on key (=target column name), value (=actual column name) mapping
        column_names_map = {v.get(ConfigYML.TRANSFORM_FROM): k for k, v in column_config.items()}
        df = df.rename(columns=column_names_map)

        # ensure all columns are there, remove additional columns, fill missing columns with nan
        n: pd.DataFrame = pd.DataFrame(columns=column_names_map.values())
        df = pd.concat([df, n], ignore_index=True)
        df = df[column_config.keys()]

        # cast datatypes based on type in config
        colum_types_map = {k: v.get(ConfigYML.TRANSFORM_TYPE) for k, v in column_config.items() if v.get(ConfigYML.TRANSFORM_TYPE)}
        df = df.astype(colum_types_map)

        # replace row values in a column based on config
        for k, v in column_config.items():
            if field := v.get(ConfigYML.TRANSFORM_REPLACE_CONTENT):
                find_value = field.get(ConfigYML.TRANSFORM_REPLACE_CONTENT_FIND)
                replace_value = field.get(ConfigYML.TRANSFORM_REPLACE_CONTENT_REPLACE)
                df[k] = df.apply(lambda x: Transformer._replace_match(x[k], find_value, replace_value), axis=1)

        return self.after_apply(df)

    def after_apply(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        hook for override
        :param df:
        :return:
        """
        return df
