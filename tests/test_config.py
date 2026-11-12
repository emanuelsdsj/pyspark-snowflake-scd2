import pytest

from scd2_pipeline.config import load_snowflake_config

ENV = {
    "SNOWFLAKE_ACCOUNT": "xy12345",
    "SNOWFLAKE_USER": "loader",
    "SNOWFLAKE_PASSWORD": "secret",
    "SNOWFLAKE_WAREHOUSE": "COMPUTE_WH",
    "SNOWFLAKE_DATABASE": "CATALOG",
    "SNOWFLAKE_SCHEMA": "PUBLIC",
    "SNOWFLAKE_ROLE": "SYSADMIN",
}


@pytest.fixture
def snowflake_env(monkeypatch):
    for key, value in ENV.items():
        monkeypatch.setenv(key, value)


def test_spark_options_build_account_url(snowflake_env):
    options = load_snowflake_config().as_spark_options()

    assert options["sfURL"] == "xy12345.snowflakecomputing.com"
    assert options["sfDatabase"] == "CATALOG"


def test_bootstrap_kwargs_skip_objects_that_may_not_exist(snowflake_env):
    kwargs = load_snowflake_config().as_bootstrap_kwargs()

    assert set(kwargs) == {"account", "user", "password", "role"}


@pytest.mark.parametrize("missing", ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_ROLE"])
def test_missing_variable_raises(snowflake_env, monkeypatch, missing):
    monkeypatch.delenv(missing)

    with pytest.raises(RuntimeError, match=missing):
        load_snowflake_config()


def test_empty_variable_is_treated_as_missing(snowflake_env, monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_USER", "")

    with pytest.raises(RuntimeError, match="SNOWFLAKE_USER"):
        load_snowflake_config()
