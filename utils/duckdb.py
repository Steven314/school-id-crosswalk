import os
from typing import List

import duckdb
import polars as pl


class DuckDB:
    """
    A wrapper around the `duckdb` library.

    Note:
        If you need to access a Python object or DuckPyRelation, don't use the
        wrapper. Go directly through `self.duck` to call the method.
    """

    def __init__(self, db_file: str = ":memory:"):
        self.db_file = db_file
        self.duck: duckdb.DuckDBPyConnection = duckdb.connect(self.db_file)  # type: ignore

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):  # type: ignore
        self.close()

    def close(self):
        self.duck.close()

    def sql(self, query: str, alias: str = "", params: object = None):
        """Pass the SQL function one level higher.

        Args:
            query (str): A SQL query
            alias (str, optional): An alias. Defaults to "".
            params (object, optional): Parameters. Defaults to None.
        """

        return self.duck.sql(query=query, alias=alias, params=params)

    def table(self, table_name: str):
        """Pass the table function one level higher.

        Args:
            table_name (str): The table.
        """

        return self.duck.table(table_name)

    def execute(self, query: str, parameters: object = None):
        """Pass the execute function one level higher.

        Args:
            query (str): The query/statement.
            parameters (object, optional): Parameters. Defaults to None.
        """

        self.duck.execute(query=query, parameters=parameters)

    def install_ext(self, ext: str, use_https: bool = False):
        if use_https:
            https_url = "https://extensions.duckdb.org"

            if not self.check_extension_installed("httpfs"):
                self.install_httpfs()

            if not self.check_extension_installed(ext):
                self.duck.install_extension(ext, repository_url=https_url)
        else:
            if not self.check_extension_installed(ext):
                self.duck.install_extension(ext)

    def load_ext(self, ext: str):
        if not self.check_extension_loaded(ext):
            self.duck.load_extension(ext)

    def install_and_load_extension(self, ext: str, use_https: bool = False):
        """
        Conditionally installs and loads a DuckDB extension.

        Args:
            ext (str): The DuckDB extension.
            use_https (bool): Use HTTPS instead of HTTP. Requires the
                [HTTPFS](https://duckdb.org/docs/stable/core_extensions/httpfs/overview)
                extension. This is to get around network issues with
                downloading over HTTP.
        """

        self.install_ext(ext, use_https)
        self.load_ext(ext)

    def check_extension_installed(self, ext: str) -> bool:
        """Checks that a DuckDB extension is installed.

        Args:
            duck (duckdb.DuckDBPyConnection): The DuckDB connection.
            ext (str): Extension name.

        Returns:
            bool: Is the extension installed?
        """

        return self.duck.execute(
            "SELECT count_star()::BOOL "
            "FROM duckdb_extensions() "
            "WHERE extension_name = $ext AND installed",
            {"ext": ext},
        ).fetchall()[0][0]

    def check_extension_loaded(self, ext: str) -> bool:
        """Checks that a DuckDB extension is loaded.

        Args:
            ext (str): Extension name.

        Returns:
            bool: Is the extension loaded?
        """

        return self.duck.execute(
            "SELECT count_star()::BOOL "
            "FROM duckdb_extensions() "
            "WHERE extension_name = $ext AND loaded",
            {"ext": ext},
        ).fetchall()[0][0]

    def install_httpfs(self):
        """
        Install the
        [HTTPFS](https://duckdb.org/docs/stable/core_extensions/httpfs/overview)
        extension.

        The version and platform are automatically found using DuckDB itself.
        """
        import os

        import requests

        platform = self.duck.execute("PRAGMA platform").fetchall()[0][0]
        version = duckdb.__version__

        file_name = "httpfs.duckdb_extension.gz"
        https_url = "https://extensions.duckdb.org"

        if not os.path.exists(file_name):
            resp = requests.get(
                f"{https_url}/v{version}/{platform}/{file_name}"
            )

            with open(file_name, mode="wb") as file:
                file.write(resp.content)

        self.duck.execute(f"INSTALL '{file_name}'")

    def table_exists(self, table_name: str) -> bool:
        return bool(
            self.duck.sql(
                "select * from duckdb_tables() where table_name = ?",
                params=[table_name],
            ).shape[0]
        )

    def view_exists(self, view_name: str) -> bool:
        return bool(
            self.duck.sql(
                "select * from duckdb_views() where view_name = ?",
                params=[view_name],
            ).shape[0]
        )

    def attach_db_dir(self, dir: str) -> pl.DataFrame:
        """Attach all DuckDB files in a directory.

        Args:
            dir (str): The directory holding the database files.

        Returns:
            pl.DataFrame: A table of the attached databases in order to confirm
                that they are all there.
        """
        for file in os.listdir(dir):
            self.attach_db(os.path.join(dir, file))

        return self.duck.sql(
            "SELECT database_name, path, readonly "
            "FROM duckdb_databases() "
            "WHERE NOT internal"
        ).pl()

    def attach_db(self, file: str):
        """Attach a DuckDB Database File

        The database attaches according to the filename.

        Args:
            duck (duckdb.DuckDBPyConnection): DuckDB connection.
            file (str): Path to the file.
        """

        if file.endswith(".duckdb"):
            self.duck.execute(f"ATTACH IF NOT EXISTS '{file}'")

    def create_fts_index(
        self,
        input_table: str,
        input_id: str,
        input_values: List[str],
        stemmer: str = "porter",
        stopwords: str = "english",
        ignore: str = "(\\.|[^a-z])+",
        strip_accents: bool | int = 1,
        lower: bool | int = 1,
        overwrite: bool | int = 0,
    ):
        sql = f"""PRAGMA create_fts_index(
            {input_table},
            {input_id},
            {", ".join(input_values)},
            stemmer = '{stemmer}',
            stopwords = '{stopwords}',
            ignore = '{ignore}',
            strip_accents = {strip_accents},
            lower = {lower},
            overwrite = {overwrite}
        )"""

        self.duck.execute(sql)

    def create_table_query(self, table_name: str, query: str):
        """Create a DuckDB Table From a SQL Query

        Args:
            table_name (str): The name of the table to be created.
            query (str): A SQL query stored as a string.
        """

        self.duck.sql(f"CREATE OR REPLACE TABLE {table_name} AS ({query})")

    def create_table_file(self, table_name: str, path: str):
        """Create a DuckDB Table From a SQL File

        Args:
            table_name (str): The name of the table to be created.
            path (str): The path to the SQL file.
        """

        with open(path) as f:
            self.duck.sql(
                f"CREATE OR REPLACE TABLE {table_name} AS ({f.read()})"
            )

    def create_view_file(self, view_name: str, path: str):
        """Create a DuckDB View From a SQL File

        Args:
            view_name (str): The name of the view to be created.
            path (str): The path to the SQL file.
        """

        with open(path) as f:
            self.duck.sql(
                f"CREATE OR REPLACE VIEW {view_name} AS ({f.read()})"
            )

    def create_view_query(self, view_name: str, query: str):
        """Create a DuckDB View From a SQL Query

        Args:
            view_name (str): The name of the view to be created.
            query (str): A SQL query stored as a string.
        """

        self.duck.sql(f"CREATE OR REPLACE VIEW {view_name} AS ({query})")

    def start_ui(self):
        self.duck.execute("call start_ui()")


if __name__ == "__main__":
    with DuckDB() as duck:
        duck.install_and_load_extension("spatial", use_https=True)
        duck.install_and_load_extension("fts", use_https=True)
