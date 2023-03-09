WITH
    final AS (
        SELECT DISTINCT km.ymr_idlink_foreign                   AS loanid,
                        pcd.rcd_currentstart                    AS value_date,
                        pcd.rcd_currentstart                    AS booking_date,
                        DATEADD(MONTH, 1, pcd.rcd_currentstart) AS first_repayment_date,
                        xtrm_idtext                             AS fee_type,
                        rtm_value                               AS amount
        FROM dbo.io_keys_masterreference      AS km
        JOIN dbo.io_product_transaction       AS pt
             ON pt.rtm_idlink_rmr = ymr_idlink_armnet
        JOIN dbo.io_control_transactionmaster AS ct
             ON xtrm_id = rtm_idlink_xtrm
        JOIN dbo.io_product_controldate       AS pcd
             ON pcd.rcd_idlink_rmr = km.ymr_idlink_armnet AND
                pcd.rcd_type = 6 -- disbursement date
        WHERE ymr_idlink_xfk = '{ef3baea0-8d36-4c5a-8a2d-6ce9e119335b}'
          AND xtrm_idtext IN ('Brokerage Fee',
                              'Establishment Fee',
                              'Transaction Fees')
    )
SELECT *
FROM final
