-- Single Applicants
SELECT 'sole' AS application_type,
       *
FROM (
    SELECT ls.loan_id,
           CONCAT('''', CAST(loan_id AS STRING), '''')                                            AS loan_id_str,
           ls.loanstatus,
           REPLACE(ls.loanstatus, 'Direct Money\\')                                               AS loan_status,
           ls.startdate,
           count_of_tran,
           ROW_NUMBER() OVER (PARTITION BY loanstatus ORDER BY count_of_tran, ls.startdate DESC ) AS loan_seq
    FROM report.rpt_arm_loanstatus        ls
    JOIN (
             SELECT km.ymr_idlink_foreign,
                    COUNT(DISTINCT pt.rtm_id) AS count_of_tran
             FROM history.v_c_arm_dbo_io_keys_masterreference km
             JOIN history.v_c_arm_dbo_io_product_transaction  pt
                  ON pt.rtm_idlink_rmr = km.ymr_idlink_armnet
             WHERE ymr_idlink_xfk = '{ef3baea0-8d36-4c5a-8a2d-6ce9e119335b}'
               AND rtm_datee < '2022-12-07'
             GROUP BY 1
         ) AS                             arm ON arm.ymr_idlink_foreign = ls.loan_id
    JOIN history.v_c_lap_dbo_wealthx_loan wl ON wl.loanid = ls.loan_id
    WHERE '2022-12-07' BETWEEN ls.startdate AND ls.enddate
      AND ls.startdate >= '2022-08-01'
      AND isjointapplication = 0
)
WHERE loan_seq <= 5
UNION ALL
-- Joint Applicants
SELECT 'joint' AS application_type,
       *
FROM (
    SELECT ls.loan_id,
           CONCAT('''', CAST(loan_id AS STRING), '''')                                            AS loan_id_str,
           ls.loanstatus,
           REPLACE(ls.loanstatus, 'Direct Money\\')                                               AS loan_status,
           ls.startdate,
           count_of_tran,
           ROW_NUMBER() OVER (PARTITION BY loanstatus ORDER BY count_of_tran, ls.startdate DESC ) AS loan_seq
    FROM report.rpt_arm_loanstatus ls
    JOIN (
             SELECT km.ymr_idlink_foreign,
                    COUNT(DISTINCT pt.rtm_id) AS count_of_tran
             FROM history.v_c_arm_dbo_io_keys_masterreference km
             JOIN history.v_c_arm_dbo_io_product_transaction  pt
                  ON pt.rtm_idlink_rmr = km.ymr_idlink_armnet
             WHERE ymr_idlink_xfk = '{ef3baea0-8d36-4c5a-8a2d-6ce9e119335b}'
               AND rtm_datee < '2022-12-07'
             GROUP BY 1
         ) AS                      arm ON arm.ymr_idlink_foreign = ls.loan_id
    JOIN (
             SELECT DISTINCT wl.loanid,
                             wl.wisrcustomerid  AS pri_id,
                             wlj.wisrcustomerid AS sec_id
             FROM history.v_c_lap_dbo_wealthx_loan wl
             JOIN history.v_c_lap_dbo_wealthx_loan wlj ON wlj.email = wl.jointapplicantemail
             WHERE lower(wl.wisrcustomerid) <> lower(wlj.wisrcustomerid)
               AND wlj.wisrcustomerid <> ''
               AND wlj.wisrcustomerid <> '00000000-0000-0000-0000-000000000000'
               AND wl.isjointapplication = 1
         ) AS                      joint ON joint.loanid = ls.loan_id
    WHERE '2022-12-07' BETWEEN ls.startdate AND ls.enddate
      AND ls.startdate >= '2022-08-01'
)
WHERE loan_seq <= 5


