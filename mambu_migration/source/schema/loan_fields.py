import pandas as pd
from mambu_migration.source.util.functions import print_tabulate


class LoanField:

    # Below for loan account
    default_loan_config_list = {
        "accountHolderType": "Group",
        "productTypeKey": "8a89876f84504b1901845aae7cdd6ef8",
        "interestRateSource": "FIXED_INTEREST_RATE",
        "accrueInterestAfterMaturity": False,
        "interestApplicationMethod": "REPAYMENT_DUE_DATE",
        "interestBalanceCalculationMethod": "PRINCIPAL_AND_INTEREST",
        "interestCalculationMethod": "DECLINING_BALANCE_DISCOUNTED",
        "interestChargeFrequency": "ANNUALIZED",
        "interestType": "SIMPLE_INTEREST",
        "principalRepaymentInterval": 1,
        "gracePeriod": 0,
        "gracePeriodType": "NONE",
        "repaymentPeriodCount": 1,
        "scheduleDueDatesMethod": "INTERVAL",
        "periodicPayment": 0,
        "repaymentPeriodUnit": "MONTHS",
        "repaymentScheduleMethod": "DYNAMIC",
        "paymentPlan": pd.Series([], dtype="float64"),
        "restrictNextDueWithdrawal": False,
    }

    loan_datetime_list = [
        "approved_date",
        "next_repayment_due_date",
        "maturity_date",
    ]

    loan_field_rename_list = {
        "loanid": "id",
        "interest_rate": "interestRate",
        "loan_term": "repaymentInstallments",
        "encodedKey": "assignedBranchKey",
        "loan_amount": "loanAmount",
    }

    loan_groupby_list = [
        "id",
        "accountHolderKey",
        "assignedBranchKey",
        "accountHolderType",
        "productTypeKey",
        "loanAmount",
    ]

    loan_fields = pd.DataFrame(
        {
            "field_set": [
                "interestSettings",
                "scheduleSettings",
                "redrawSettings",
                "_additional_loan_details",
                "_customer_bank_account_details",
                "_repayment_details",
                "_adhoc_repayment_details",
            ],
            "field_id": [
                [
                    "interestRateSource",
                    "accrueInterestAfterMaturity",
                    "interestApplicationMethod",
                    "interestBalanceCalculationMethod",
                    "interestCalculationMethod",
                    "interestChargeFrequency",
                    "interestRate",
                    "interestType",
                ],
                [
                    "principalRepaymentInterval",
                    "gracePeriod",
                    "gracePeriodType",
                    "repaymentInstallments",
                    "repaymentPeriodCount",
                    "scheduleDueDatesMethod",
                    "periodicPayment",
                    "repaymentPeriodUnit",
                    "repaymentScheduleMethod",
                    "paymentPlan",
                ],
                ["restrictNextDueWithdrawal"],
                [
                    "approved_date",
                    "is_secure",
                    "loan_purpose",
                    "rate_type",
                    "test_group",
                    "loan_status",
                    "maturity_date",
                ],
                ["account_name", "account_number", "account_bsb"],
                [
                    "next_repayment_due_date",
                    "payment_frequency",
                    "repayment_amount",
                    "recurring_repayment_day",
                ],
                [
                    "adhoc_repayment_date",
                    "adhoc_repayment_amount",
                ],
            ],
            "json_array": [False, False, False, False, False, True, True],
        }
    )

    # Below for loan approval
    loan_approval = {"action": "APPROVE", "notes": ""}

    undo_loan_approval = {"action": "Undo_Approve", "notes": ""}

    # Below for loan disbursement
    disbursement_fee_key = {
        "Establishment Fee": "8a8987ca859beaf001859dfdb4e24dfe",
        "Transaction Fees": "8a8987ca859beaf001859dfdb4e24dff",
        "Brokerage Fee": "8a8987ca859beaf001859e078b374f37",
    }

    default_disbursement_config_list = {"transactionChannelId": "settlement"}

    disbursement_datetime_list = [
        "value_date",
        "booking_date",
        "first_repayment_date",
    ]

    disbursement_field_rename_list = {
        "loanid": "id",
        "value_date": "valueDate",
        "booking_date": "bookingDate",
        "first_repayment_date": "firstRepaymentDate",
    }

    disbursement_groupby_list = ["id", "bookingDate", "valueDate", "firstRepaymentDate"]

    disbursement_fields = pd.DataFrame(
        {
            "field_set": ["fees", "transactionDetails"],
            "field_id": [
                [
                    "predefinedFeeKey",
                    "amount",
                ],
                [
                    "transactionChannelId",
                ],
            ],
            "json_array": [True, False],
        }
    )
