import os
from typing import List

import duckdb
import polars as pl


def install(
    duck: duckdb.DuckDBPyConnection, ext: str, use_https: bool = False
):
    if use_https:
        https_url = "https://extensions.duckdb.org"

        if not check_installed(duck, "httpfs"):
            install_httpfs(duck)

        if not check_installed(duck, ext):
            duck.install_extension(ext, repository_url=https_url)
    else:
        if not check_installed(duck, ext):
            duck.install_extension(ext)


def load(duck: duckdb.DuckDBPyConnection, ext: str):
    if not check_loaded(duck, ext):
        duck.load_extension(ext)


def install_and_load(
    duck: duckdb.DuckDBPyConnection, ext: str, use_https: bool = False
):
    """
    Conditionally installs and loads a DuckDB extension.

    Args:
        duck (duckdb.DuckDBPyConnection): DuckDB connection.
        ext (str): The DuckDB extension.
        use_https (bool): Use HTTPS instead of HTTP. Requires the
            [HTTPFS](https://duckdb.org/docs/stable/core_extensions/httpfs/overview)
            extension. This is to get around network issues with downloading
            over HTTP.
    """

    install(duck, ext, use_https)
    load(duck, ext)


def check_installed(duck: duckdb.DuckDBPyConnection, ext: str) -> bool:
    """Checks that a DuckDB extension is installed.

    Args:
        duck (duckdb.DuckDBPyConnection): The DuckDB connection.
        ext (str): Extension name.

    Returns:
        bool: Is the extension installed?
    """

    return duck.execute(
        "SELECT count_star()::BOOL "
        "FROM duckdb_extensions() "
        "WHERE extension_name = $ext AND installed",
        {"ext": ext},
    ).fetchall()[0][0]


def check_loaded(duck: duckdb.DuckDBPyConnection, ext: str) -> bool:
    """Checks that a DuckDB extension is loaded.

    Args:
        duck (duckdb.DuckDBPyConnection): The DuckDB connection.
        ext (str): Extension name.

    Returns:
        bool: Is the extension loaded?
    """

    return duck.execute(
        "SELECT count_star()::BOOL "
        "FROM duckdb_extensions() "
        "WHERE extension_name = $ext AND loaded",
        {"ext": ext},
    ).fetchall()[0][0]


def install_httpfs(duck: duckdb.DuckDBPyConnection):
    """
    Install the
    [HTTPFS](https://duckdb.org/docs/stable/core_extensions/httpfs/overview)
    extension.

    The version and platform are automatically found using DuckDB itself.

    Args:
        duck (duckdb.DuckDBPyConnection): The DuckDB connection.
    """
    import os

    import requests

    platform = duck.execute("PRAGMA platform").fetchall()[0][0]
    version = duckdb.__version__

    file_name = "httpfs.duckdb_extension.gz"
    https_url = "https://extensions.duckdb.org"

    if not os.path.exists(file_name):
        resp = requests.get(f"{https_url}/v{version}/{platform}/{file_name}")

        with open(file_name, mode="wb") as file:
            file.write(resp.content)

    duckdb.execute(f"INSTALL '{file_name}'")


def table_exists(duck: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    return bool(
        duck.sql(
            "select * from duckdb_tables() where table_name = ?",
            params=[table_name],
        ).shape[0]
    )


def attach_db_dir(duck: duckdb.DuckDBPyConnection, dir: str) -> pl.DataFrame:
    """Attach all DuckDB files in a directory.

    Args:
        duck (duckdb.DuckDBPyConnection): The DuckDB connection.
        dir (str): The directory holding the database files.

    Returns:
        pl.DataFrame: A table of the attached databases in order to confirm
            that they are all there.
    """
    for file in os.listdir(dir):
        attach_db(duck, os.path.join(dir, file))

    return duck.sql(
        "SELECT database_name, path, readonly "
        "FROM duckdb_databases() "
        "WHERE NOT internal"
    ).pl()


def attach_db(duck: duckdb.DuckDBPyConnection, file: str):
    """Attach a DuckDB Database File

    The database attaches according to the filename.

    Args:
        duck (duckdb.DuckDBPyConnection): DuckDB connection.
        file (str): Path to the file.
    """

    if file.endswith(".duckdb"):
        duck.execute(f"ATTACH IF NOT EXISTS '{file}'")


def create_fts_index(
    duck: duckdb.DuckDBPyConnection,
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
    # duck.execute(
    #     """
    #     PRAGMA create_fts_index(
    #         input_table := $input_table,
    #         input_id := $input_id,
    #         $input_values,
    #         stemmer := $stemmer,
    #         stopwords := $stopwords,
    #         ignore := $ignore,
    #         strip_accents := $strip_accents,
    #         lower := $lower,
    #         overwrite := $overwrite
    #     )
    #     """,
    #     parameters={
    #         "input_table": input_table,
    #         "input_id": input_id,
    #         "input_values": ", ".join(input_values),
    #         "stemmer": stemmer,
    #         "stopwords": stopwords,
    #         "ignore": ignore,
    #         "strip_accents": strip_accents,
    #         "lower": lower,
    #         "overwrite": overwrite,
    #     },
    # )

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

    duck.execute(sql)


def create_table_query(
    duck: duckdb.DuckDBPyConnection, table_name: str, query: str
):
    """Create a DuckDB Table From a SQL File

    Args:
        duck (duckdb.DuckDBPyConnection): DuckDB connection.
        table_name (str): The name of the table to be created.
        query (str): A SQL query stored as a string.
    """
    duck.sql(f"CREATE OR REPLACE TABLE {table_name} AS ({query})")


def create_table_file(
    duck: duckdb.DuckDBPyConnection, table_name: str, path: str
):
    """Create a DuckDB Table From a SQL File

    Args:
        duck (duckdb.DuckDBPyConnection): DuckDB connection.
        table_name (str): The name of the table to be created.
        path (str): The path to the SQL file.
    """
    with open(path) as f:
        duck.sql(f"CREATE OR REPLACE TABLE {table_name} AS ({f.read()})")


if __name__ == "__main__":
    with duckdb.connect() as duck:  # type: ignore
        install_and_load(duck, "spatial", use_https=True)
        install_and_load(duck, "fts", use_https=True)
