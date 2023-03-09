import json
from http.client import responses
import pandas as pd
import requests

from mambu_migration.definition import root_dir
from mambu_migration.source.config import MambuConfig
from mambu_migration.source.schema.client_fields import ClientField
from mambu_migration.source.util.connector import (
    ArmConnection,
    SnowflakeConnection,
)
from mambu_migration.source.util.functions import (
    flatten_json_to_df,
    convert_df_to_json,
    get_sql,
)


class Client:
    # Get sql query
    client_details_arm = get_sql(root_dir, "client_details_arm")
    client_details_lap = get_sql(root_dir, "client_details_lap")

    def __init__(self, loan_ids):
        self.url = MambuConfig.base_url + "/clients/"
        self.loan_ids_str = ",".join(map(str, loan_ids))
        self.filter = (
            f"{MambuConfig.filter_prefix}{self.loan_ids_str}{MambuConfig.filter_suffix}"
        )
        (
            self.client_stg,
            self.client_master,
            self.client_merged,
            self.client_json,
            self.client_json_parsed,
        ) = self.get_client_data()
        self.mambu_client_list = self.client_master["id"].tolist()

    def get_client_data(self):
        # Get client data from sources

        df_cust_lap = MambuConfig.get_wisr_data(
            connection=SnowflakeConnection.get_snowflake_connection(),
            sql_filter=self.filter,
            sql_query=self.client_details_lap,
        )

        df_cust_arm = MambuConfig.get_wisr_data(
            connection=ArmConnection.get_arm_connection(),
            sql_filter=self.filter,
            sql_query=self.client_details_arm,
        )

        # Data merge and cleanup
        df_cust_stg = df_cust_lap.merge(df_cust_arm, on=["loanid", "birthdate"])

        df_cust_final = df_cust_stg.rename(
            ClientField.client_field_rename_list,
            axis=1,
        ).drop(["loanid", "application_role"], axis=1)

        df_cust_final["birthDate"] = df_cust_final["birthDate"].astype(str)

        # Apply JSON index

        df_cust_merged = df_cust_final[ClientField.client_groupby_list].copy()

        for z in ClientField.client_fields.index:
            df = MambuConfig.apply_json_index(
                df=df_cust_final,
                groupby_list=ClientField.client_groupby_list,
                nested_field_list=ClientField.client_fields.iloc[z]["field_id"],
                index_name=ClientField.client_fields.iloc[z]["field_set"],
                json_array=ClientField.client_fields.iloc[z]["json_array"],
            )
            df_cust_merged = df_cust_merged.merge(
                df, how="inner", on=ClientField.client_groupby_list
            )

        df_cust_json = convert_df_to_json(df_cust_merged)
        df_cust_json_parsed = json.loads(df_cust_json)

        return (
            df_cust_stg,
            df_cust_final,
            df_cust_merged,
            df_cust_json,
            df_cust_json_parsed,
        )

    def create_mambu_clients(self):
        # Append status and created client to result_df
        result_df = MambuConfig().create_mambu_entity(
            parsed_json=self.client_json_parsed, url=self.url
        )

        return result_df

    def fetch_mambu_clients(self, details_level="Full", limit="1000"):
        df_cust_additional = self.client_stg[["id", "loanid", "application_role"]]

        df_cust_fetched_stg = MambuConfig().fetch_mambu_entity(
            id_list=self.mambu_client_list,
            url=self.url,
            details_level=details_level,
            limit=limit,
        )

        df_cust_fetched = df_cust_additional.merge(
            df_cust_fetched_stg, left_index=True, right_index=True
        )

        return df_cust_fetched

    def delete_mambu_clients(self):
        MambuConfig().delete_mambu_entity(
            id_list=self.mambu_client_list,
            url=self.url,
            entity_name="Client ",
        )
