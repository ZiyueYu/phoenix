import pandas as pd


class ClientField:
    client_field_rename_list = {
        "firstname": "firstName",
        "middlename": "middleName",
        "surname": "lastName",
        "mobile": "mobilePhone",
        "email": "emailAddress",
        "birthdate": "birthDate",
    }

    client_groupby_list = [
        "id",
        "firstName",
        "middleName",
        "lastName",
        "birthDate",
        "mobilePhone",
        "emailAddress",
    ]

    client_fields = pd.DataFrame(
        {
            "field_set": ["addresses", "_Additional_Client_Details"],
            "field_id": [
                [
                    "line1",
                    "city",
                    "region",
                    "postcode",
                    "country",
                ],
                [
                    "veda_score",
                    "title",
                    "wisrcustomerid",
                ],
            ],
            "json_array": [True, False],
        }
    )
