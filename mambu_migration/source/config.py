import os
import pandas as pd
import requests
from flatten_json import flatten
from http.client import responses
from mambu_migration.source.util.authentication import Authentication
from mambu_migration.source.util.functions import flatten_json_to_df
from mambu_migration.source.schema.group_fields import GroupField


class Config:
    snowflake_account = os.environ["MY_SNOWFLAKE_ACCT"]
    snowflake_user = os.environ["MY_SNOWFLAKE_USER"]
    snowflake_password = os.environ["MY_SNOWFLAKE_PWD"]
    snowflake_role = os.environ["MY_SNOWFLAKE_ROLE"]
    snowflake_database = os.environ["MY_SNOWFLAKE_DB"]
    snowflake_warehouse = os.environ["MY_SNOWFLAKE_WAREHOUSE"]
    arm_host = os.environ["ARM_HOST"]
    arm_port = os.environ["ARM_PORT"]
    arm_db = os.environ["ARM_DB"]
    arm_user = os.environ["ARM_USER"]
    arm_pwd = os.environ["ARM_PWD"]


class MambuConfig:
    # API Config
    base_url = "https://wisrautest.sandbox.mambu.com/api"
    content_type = "application/json"
    accept = "application/vnd.mambu.v2+json"

    json_headers = {
        "apiKey": Authentication.get_api_key(),
        "Content-Type": content_type,
        "Accept": accept,
    }

    # SQL Filter
    filter_prefix = "WHERE loanid in ("
    filter_suffix = ")"

    @staticmethod
    def get_group_role_name_key(role):
        return GroupField().group_role_key[str(role).lower()]

    def get_funder_key(self):
        url = self.base_url + "/branches"
        headers = {"apikey": Authentication.get_api_key(), "ACCEPT": self.accept}
        df_funders = pd.DataFrame(requests.get(url, headers=headers).json())[
            ["encodedKey", "name"]
        ].rename({"name": "funder"}, axis=1)

        return df_funders

    @staticmethod
    ## Todo: below method exclude the nested json only if all values are null, consider scenarios in custom fields if 1 or muliple is null
    def apply_json_index(
        df,
        groupby_list,
        nested_field_list,
        index_name,
        json_array=False,
        # True with square bracket (custom field type = grouped), False with none (standard)
    ):
        # Below code will drop the row if field is null
        # df_no_null = df.dropna(subset=nested_field_list, how="all")
        # if len(df_no_null) == 0:
        #     return df[groupby_list]
        # df_filled = df.fillna(value={field: "" for field in nested_field_list})
        if not json_array:
            df_grouped = (
                df.groupby(groupby_list, group_keys=False)
                .apply(lambda x: x[nested_field_list].to_dict(orient="records")[0])
                .apply(
                    lambda x: None
                    if all(v is None for v in x.values())
                    else {k: v for k, v in x.items() if v is not None}
                )
                # .apply(lambda x: {k: v for k, v in x.items() if v != None})
                .reset_index(name=index_name)
            )
        else:
            df_grouped = (
                df.groupby(groupby_list, group_keys=False)
                .apply(lambda x: x[nested_field_list].to_dict(orient="records"))
                .apply(
                    lambda x: [
                        None
                        if all(v is None or v == "nan" for v in d.values())
                        else {k: v for k, v in d.items() if v is not None}
                        for d in x
                    ]
                )
                .reset_index(name=index_name)
            )
            df_grouped = df_grouped.apply(
                lambda x: [None if value == [None] else value for value in x]
            )

        return df_grouped

    @staticmethod
    def get_wisr_data(connection, sql_filter, sql_query):
        with connection as conn:
            df = pd.read_sql(f"{sql_query}{sql_filter}", conn)
            return df

    def create_mambu_entity(self, parsed_json, url):
        response_data = []
        status_code = []
        status = []

        for key, obj in parsed_json.items():
            response = requests.post(url, headers=self.json_headers, json=obj)
            status.append(responses[response.status_code])
            status_code.append(response.status_code)
            response_data.append(flatten(response.json()))
        df = pd.concat(
            [
                pd.DataFrame(status, columns=["status"]),
                pd.DataFrame(status_code, columns=["status code"]),
                pd.DataFrame(response_data),
            ],
            axis=1,
        )

        return df

    def fetch_mambu_entity(self, id_list, url, details_level, limit):
        payload = {"detailsLevel": details_level, "limit": limit}

        df_fetched = pd.DataFrame()
        for value in id_list:
            response = requests.get(
                url + str(value),
                headers=self.json_headers,
                params=payload,
            )

            df_fetched = pd.concat(
                [df_fetched, flatten_json_to_df(response.json())],
                ignore_index=True,
            )
        return df_fetched

    # def fetch_mambu_entity2(self, data, url, details_level, limit):
    #     payload = {"detailsLevel": details_level, "limit": limit}
    #
    #     df_fetched = pd.DataFrame()
    #     for key in data.index:
    #         response = requests.get(
    #             url + data.loc[key, "id"],
    #             headers=MambuConfig.json_headers,
    #             params=payload,
    #             )
    #
    #         df_stg = pd.merge(
    #             pd.DataFrame(data.loc[key])
    #             .transpose()
    #             .reset_index(drop=True),
    #             flatten_json_to_df(response.json()),
    #             left_index=True,
    #             right_index=True,
    #             )
    #
    #         df_fetched = pd.concat(
    #             [df_fetched, df_stg], ignore_index=True
    #         )
    #     return df_fetched

    def delete_mambu_entity(self, url, entity_name, ids_list):
        for value in ids_list:
            response = requests.delete(url + str(value), headers=self.json_headers)
            if response.status_code == 204:
                print(entity_name + str(value) + " is deleted")
            else:
                print(responses[response.status_code])
