import json
from http.client import responses

import pandas as pd
import requests
from mambu_migration.source.client import Client
from mambu_migration.source.config import MambuConfig
from mambu_migration.source.util.functions import convert_df_to_json
from mambu_migration.source.util.authentication import Authentication
from mambu_migration.source.util.functions import flatten_json_to_df


class Group:
    def __init__(self, loan_ids):
        self.loan_ids = loan_ids
        (self.group_master, self.group_json_parsed) = self.get_group_master()
        self.mambu_groups = self.fetch_mambu_groups()

    def get_group_master(self):
        # Get Mambu and cleanup the code in format required by Mambu
        df_group_final = (
            Client(loan_ids=self.loan_ids)
            .mambu_clients.assign(
                groupRoleNameKey=lambda x: x.apply(
                    lambda y: MambuConfig().get_group_role_name_key(
                        y["application_role"]
                    ),
                    axis=1,
                )
            )[["loanid", "encodedKey", "groupRoleNameKey"]]
            .assign(groupName=lambda x: x["loanid"])
            .rename({"loanid": "id", "encodedKey": "clientKey"}, axis=1)
            .assign(groupName=lambda x: x.pop("groupName"))
        )

        df_group_json = (
            df_group_final.assign(
                roles=lambda x: x.apply(
                    lambda y: [{"groupRoleNameKey": y["groupRoleNameKey"]}], axis=1
                )
            )
            .groupby(["groupName", "id"])
            .apply(lambda x: x[["clientKey", "roles"]].to_dict(orient="records"))
            .reset_index(name="groupMembers")
            .to_json(orient="index")
        )

        df_group_json_parsed = json.loads(df_group_json)

        return df_group_final, df_group_json_parsed

    def create_mambu_groups(self):
        url = MambuConfig.base_url + "/groups"

        result_df = MambuConfig().create_mambu_entity(
            parsed_json=self.group_json_parsed, url=url
        )

        return result_df

    def fetch_mambu_groups(self, details_level="Full", limit="1000"):
        url = MambuConfig.base_url + "/groups/"

        id_list = self.group_master["id"].tolist()

        df_group_fetched = MambuConfig().fetch_mambu_entity(
            id_list=id_list, url=url, details_level=details_level, limit=limit
        )

        return df_group_fetched

    def delete_mambu_groups(self):
        url = MambuConfig.base_url + "/groups/"

        group_delete_list = self.group_master["id"].drop_duplicates().tolist()

        MambuConfig().delete_mambu_entity(
            url=url, entity_name="Group ", ids_list=group_delete_list
        )
