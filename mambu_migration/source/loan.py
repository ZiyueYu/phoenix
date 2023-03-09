import json
from http.client import responses
import pandas as pd
import requests
import numpy as np

from mambu_migration.definition import root_dir
from mambu_migration.source.config import MambuConfig
from mambu_migration.source.group import Group
from mambu_migration.source.util.connector import (
    ArmConnection,
    SnowflakeConnection,
)
from mambu_migration.source.util.functions import (
    flatten_json_to_df,
    convert_df_to_json,
    get_sql,
    add_sydney_timezone,
)
from mambu_migration.source.schema.loan_fields import LoanField


class LoanAccount:
    loan_accounts_arm = get_sql(root_dir, "loan_accounts_arm")
    loan_accounts_lap = get_sql(root_dir, "loan_accounts_lap")
    loan_disbursement_arm = get_sql(root_dir, "loan_disbursement_details")

    def __init__(self, loan_ids, test_group):
        self.url = MambuConfig.base_url + "/loans/"
        self.loan_ids = loan_ids
        self.loan_ids_str = ",".join(map(str, loan_ids))
        self.filter = (
            f"{MambuConfig.filter_prefix}{self.loan_ids_str}{MambuConfig.filter_suffix}"
        )
        self.test_group = test_group
        # Below related to loan account
        (
            self.loan_stg,
            self.loan_master,
            self.loan_merged,
            self.loan_json,
            self.loan_json_parsed,
        ) = self.get_loan_account_data()
        self.mambu_loan_list = self.loan_master["id"].tolist()
        # below related to loan account disbursement
        (
            self.disbursement_stg,
            self.disbursement_master,
            self.disbursement_merged,
            self.disbursement_json,
            self.disbursement_json_parsed,
        ) = self.get_loan_disbursement_data()

    def get_loan_account_data(self):
        # Get loan details from LAP
        df_loan_lap = MambuConfig.get_wisr_data(
            connection=SnowflakeConnection.get_snowflake_connection(),
            sql_filter=self.filter,
            sql_query=self.loan_accounts_lap,
        )

        # Get loan details from ARM
        df_loan_arm = MambuConfig.get_wisr_data(
            connection=ArmConnection.get_arm_connection(),
            sql_filter=self.filter,
            sql_query=self.loan_accounts_arm,
        )

        # Get funder keys from Mambu
        df_funder = MambuConfig().get_funder_key()

        # Get group key from Mambu
        df_group = (
            Group(loan_ids=self.loan_ids)
            .fetch_mambu_groups()[["encodedKey", "id"]]
            .rename({"encodedKey": "accountHolderKey", "id": "loanid"}, axis=1)
            .astype({"loanid": "int64"})
        )

        # Merge all data into one DataFrame
        df_loan_stg = (
            df_loan_arm.merge(df_loan_lap, on=["loanid"])
            .merge(df_funder, on=["funder"])
            .merge(df_group, on=["loanid"])
            .assign(test_group=self.test_group)
            .drop_duplicates()
            .reset_index(drop=True)
        )

        # Add default product config parameter
        for key, value in LoanField().default_loan_config_list.items():
            df_loan_stg[key] = value

        # Update datetime column to have timezone information
        for datetime in df_loan_stg[LoanField().loan_datetime_list]:
            df_loan_stg[datetime] = (
                df_loan_stg[datetime]
                .astype(str)
                .apply(lambda x: add_sydney_timezone(input_datetime=x))
            )

        df_loan_final = df_loan_stg.rename(
            LoanField().loan_field_rename_list,
            axis=1,
        ).drop("funder", axis=1)

        # Replace all kinds of null with None
        df_loan_final = df_loan_final.replace({np.nan: None})

        # Apply JSON index
        df_loan_merged = df_loan_final[LoanField.loan_groupby_list].copy()

        for z in LoanField.loan_fields.index:
            df = MambuConfig.apply_json_index(
                df=df_loan_final,
                groupby_list=LoanField().loan_groupby_list,
                nested_field_list=LoanField.loan_fields.iloc[z]["field_id"],
                index_name=LoanField.loan_fields.iloc[z]["field_set"],
                json_array=LoanField.loan_fields.iloc[z]["json_array"],
            )
            df_loan_merged = df_loan_merged.merge(
                df, how="inner", on=LoanField.loan_groupby_list
            )

        # Convert to json
        df_loan_json = convert_df_to_json(df_loan_merged)
        df_loan_json_parsed = json.loads(df_loan_json)

        return (
            df_loan_stg,
            df_loan_final,
            df_loan_merged,
            df_loan_json,
            df_loan_json_parsed,
        )

    def create_mambu_loans(self):
        # Append status and created client to result_df
        result_df = MambuConfig().create_mambu_entity(
            parsed_json=self.loan_json_parsed, url=self.url
        )

        return result_df

    def fetch_mambu_loans(self, details_level="Full", limit="1000"):
        df_loan_fetched = MambuConfig().fetch_mambu_entity(
            id_list=self.mambu_loan_list,
            url=self.url,
            details_level=details_level,
            limit=limit,
        )

        return df_loan_fetched

    def delete_mambu_loans(self):
        # API call to delete mambu clients
        MambuConfig().delete_mambu_entity(
            id_list=self.mambu_loan_list,
            url=self.url,
            entity_name="Loan ",
        )

    def approve_mambu_loans(self):
        MambuConfig().change_loan_state(
            id_list=self.mambu_loan_list,
            url=self.url,
            action=LoanField.loan_approval,
            action_name="approved",
        )

    def undo_mambu_loan_approvals(self):
        MambuConfig().change_loan_state(
            id_list=self.mambu_loan_list,
            url=self.url,
            action=LoanField.undo_loan_approval,
            action_name="undo approved",
        )

    def get_loan_disbursement_data(self):
        df_disburse_arm = MambuConfig.get_wisr_data(
            connection=ArmConnection.get_arm_connection(),
            sql_filter=self.filter,
            sql_query=self.loan_disbursement_arm,
        )

        df_disburse_stg = df_disburse_arm.assign(
            predefinedFeeKey=lambda x: x.apply(
                lambda y: MambuConfig().get_disbursement_fee_key(y["fee_type"]),
                axis=1,
            )
        )

        for key, value in LoanField().default_disbursement_config_list.items():
            df_disburse_stg[key] = value

        for datetime in df_disburse_stg[LoanField().disbursement_datetime_list]:
            df_disburse_stg[datetime] = (
                df_disburse_stg[datetime]
                .astype(str)
                .apply(lambda x: add_sydney_timezone(input_datetime=x))
            )

        df_disburse_final = df_disburse_stg.rename(
            LoanField().disbursement_field_rename_list,
            axis=1,
        ).drop("fee_type", axis=1)

        df_disburse_merged = df_disburse_final[
            LoanField.disbursement_groupby_list
        ].copy()

        for z in LoanField.disbursement_fields.index:
            df = MambuConfig.apply_json_index(
                df=df_disburse_final,
                groupby_list=LoanField().disbursement_groupby_list,
                nested_field_list=LoanField.disbursement_fields.iloc[z]["field_id"],
                index_name=LoanField.disbursement_fields.iloc[z]["field_set"],
                json_array=LoanField.disbursement_fields.iloc[z]["json_array"],
            )
            df_disburse_merged = df_disburse_merged.merge(
                df, how="inner", on=LoanField.disbursement_groupby_list
            )
        df_disburse_merged = df_disburse_merged.drop_duplicates(
            subset=LoanField().disbursement_groupby_list, ignore_index=True
        )

        df_disburse_json = convert_df_to_json(df_disburse_merged)
        df_disburse_json_parsed = json.loads(df_disburse_json)

        return (
            df_disburse_stg,
            df_disburse_final,
            df_disburse_merged,
            df_disburse_json,
            df_disburse_json_parsed
        )

    def disburse_mambu_loan(self):
        # Append status and created client to result_df
        result_df = MambuConfig().disburse_mambu_loan(
            parsed_json=self.disbursement_json_parsed, url=self.url
        )

        return result_df
