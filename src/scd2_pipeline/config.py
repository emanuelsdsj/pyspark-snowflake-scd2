import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class SnowflakeConfig:
    account: str
    user: str
    password: str
    warehouse: str
    database: str
    schema: str
    role: str

    def as_spark_options(self) -> dict[str, str]:
        return {
            "sfURL": f"{self.account}.snowflakecomputing.com",
            "sfUser": self.user,
            "sfPassword": self.password,
            "sfWarehouse": self.warehouse,
            "sfDatabase": self.database,
            "sfSchema": self.schema,
            "sfRole": self.role,
        }

    def as_connector_kwargs(self) -> dict[str, str]:
        return {
            "account": self.account,
            "user": self.user,
            "password": self.password,
            "warehouse": self.warehouse,
            "database": self.database,
            "schema": self.schema,
            "role": self.role,
        }

    def as_bootstrap_kwargs(self) -> dict[str, str]:
        """Connection kwargs without warehouse/database/schema, for first-time
        setup before those objects exist."""
        return {
            "account": self.account,
            "user": self.user,
            "password": self.password,
            "role": self.role,
        }


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def load_snowflake_config() -> SnowflakeConfig:
    return SnowflakeConfig(
        account=_require_env("SNOWFLAKE_ACCOUNT"),
        user=_require_env("SNOWFLAKE_USER"),
        password=_require_env("SNOWFLAKE_PASSWORD"),
        warehouse=_require_env("SNOWFLAKE_WAREHOUSE"),
        database=_require_env("SNOWFLAKE_DATABASE"),
        schema=_require_env("SNOWFLAKE_SCHEMA"),
        role=_require_env("SNOWFLAKE_ROLE"),
    )
