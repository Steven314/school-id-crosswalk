###
#
# This facilitates the installation and loading of the required DuckDB
# extensions.
#
###


import duckdb


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

    if use_https:
        https_url = "https://extensions.duckdb.org"

        if not check_installed(duck, "httpfs"):
            install_httpfs(duck)

        if not check_installed(duck, ext):
            duck.install_extension(ext, repository_url=https_url)
    else:
        if not check_installed(duck, ext):
            duck.install_extension(ext)

    if not check_loaded(duck, ext):
        duck.load_extension(ext)


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
    import requests
    import os

    platform = duck.execute("PRAGMA platform").fetchall()[0][0]
    version = duckdb.__version__

    file_name = "httpfs.duckdb_extension.gz"
    https_url = "https://extensions.duckdb.org"

    if not os.path.exists(file_name):
        resp = requests.get(f"{https_url}/v{version}/{platform}/{file_name}")

        with open(file_name, mode="wb") as file:
            file.write(resp.content)

    duckdb.execute(f"INSTALL '{file_name}'")


if __name__ == "__main__":
    with duckdb.connect() as duck:  # type: ignore
        install_and_load(duck, "spatial", use_https=True)
        install_and_load(duck, "fts", use_https=True)
