from contextlib import contextmanager
from sqlalchemy import create_engine
from mambu_migration.source.config import Config
import sqlalchemy.engine as se
import snowflake.sqlalchemy as sf


class ArmConnection:
    """
    Initialise connection to ARM Database
    """

    @staticmethod
    @contextmanager
    def get_arm_connection(
            config: Config = Config()
    ):
        engine = create_engine(se.URL.create(drivername='mssql+pymssql',
                                             username=config.arm_user,
                                             password=config.arm_pwd,
                                             host=config.arm_host,
                                             port=config.arm_port,
                                             database=config.arm_db))
        connection = engine.connect()
        try:
            yield connection
        finally:
            connection.close()


class SnowflakeConnection:
    """
    Initialise connection to Snowflake Database
    """

    @staticmethod
    @contextmanager
    def get_snowflake_connection(
            config: Config = Config()
    ):
        engine = create_engine(sf.URL(
            account=config.snowflake_account,
            user=config.snowflake_user,
            password=config.snowflake_password,
            database=config.snowflake_database,
            warehouse=config.snowflake_warehouse,
            role=config.snowflake_role,
        ))

        connection = engine.connect()
        try:
            yield connection
        finally:
            connection.close()
