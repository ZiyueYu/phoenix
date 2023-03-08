WITH
    pri_applicant AS (
        SELECT wisrcustomerid,
/*               INITCAP(firstname)  AS firstname,
               INITCAP(middlename) AS middlename,
               INITCAP(surname)    AS surname,*/
               applicationid,
               dob :: DATE         AS birthdate,
               loanid,
               'primary'           AS applicant_role
        FROM history.v_c_lap_dbo_wealthx_loan AS wl
    ),
    all_applicant AS (
        SELECT pa.wisrcustomerid,
               REPLACE(pa.wisrcustomerid, '-', '')      AS id,
/*               INITCAP(REPLACE(pa.firstname, ' ', ''))  AS firstname,
               INITCAP(REPLACE(pa.middlename, ' ', '')) AS middlename,
               INITCAP(REPLACE(pa.surname, ' ', ''))    AS surname,*/
               birthdate,
               pa.loanid,
               'primary'                                AS application_role
        FROM pri_applicant AS pa
        UNION
        SELECT pa.wisrcustomerid,
               REPLACE(wl.wisrcustomerid, '-', '')      AS id,
/*               INITCAP(REPLACE(wl.firstname, ' ', ''))  AS firstname,
               INITCAP(REPLACE(wl.middlename, ' ', '')) AS middlename,
               INITCAP(REPLACE(wl.surname, ' ', ''))    AS surname,*/
               wl.dob :: DATE                           AS birthdate,
               pa.loanid,
               'secondary'                              AS application_role
        FROM pri_applicant                    AS pa
        JOIN history.v_c_lap_dbo_wealthx_loan AS wl
             ON wl.applicationid = pa.applicationid AND wl.loanid <> pa.loanid
    )
SELECT *
FROM all_applicant
