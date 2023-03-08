  WITH arm_loan_final AS (
      SELECT CAST(km.ymr_idlink_foreign AS INT)           AS loanid,
             funder.cmr_name                              AS funder,
             pcra.rcd_currentstart                        AS approved_date,
             rcr_rate                                     AS interest_rate,
             rlm_term_years * 12                          AS loan_term,
             rcb_currentvalue                             AS loan_amount,
             payment_frequency,
             recurring_repayment_day,
             REPLACE(css.xsu_detail, 'Direct Money\', '') AS loan_status,
             scheduled_payment.next_repayment_due_date,
             scheduled_payment.repayment_amount,
             oneoff_payment.adhoc_repayment_date,
             oneoff_payment.adhoc_repayment_amount,
             cbd.cbd_accountname                          AS account_name,
             cbd.cbd_accountbsbno                         AS account_bsb,
             cbd_accountnumber                            AS account_number,
             pcrm.rcd_currentstart                        AS maturity_date
        FROM dbo.io_keys_masterreference         AS km
        JOIN      dbo.io_product_masterreference AS pm
                  ON pm.rmr_id = km.ymr_idlink_armnet
        JOIN      dbo.io_product_controlbalance  AS pcb
                  ON pcb.rcb_idlink_rmr = km.ymr_idlink_armnet
        JOIN      dbo.io_control_productbalance  AS cpb
                  ON cpb.xrbl_id = pcb.rcb_idlink_xrbl AND
                     xrbl_id = '{2d21b537-8ee3-4cc6-9a82-e50a27305db5}' /* Loan\Standard\Advanced*/
        JOIN      (
                      SELECT lmr_idlink_code_id,
                             cmr_name
                        FROM dbo.io_link_masterreference
                        INNER JOIN dbo.io_product_masterreference
                                   ON rmr_id = lmr_idlink_code_id
                        INNER JOIN dbo.io_client_masterreference
                                   ON cmr_id = lmr_idlink_cmr
                        INNER JOIN dbo.io_control_productratemaster
                                   ON xrrm_detail LIKE
                                      CONCAT('Direct Money\Provisional Rate\', cmr_name)
                       WHERE lmr_idlink_association = '{e14b0493-faff-445b-ba7d-bb2c94b25d7c}'
                  )                              AS funder
                  ON funder.lmr_idlink_code_id = km.ymr_idlink_armnet
        JOIN      dbo.io_product_controldate     AS pcrd
                  ON pcrd.rcd_idlink_rmr = km.ymr_idlink_armnet AND pcrd.rcd_type = 6 /* Disbursement Date */
        JOIN      dbo.io_product_controldate     AS pcra
                  ON pcra.rcd_idlink_rmr = km.ymr_idlink_armnet AND pcra.rcd_type = 2 /* Approved Date */
        JOIN      dbo.io_product_controldate     AS pcrm
                  ON pcrm.rcd_idlink_rmr = km.ymr_idlink_armnet AND pcrm.rcd_type = 4 /* Maturity Date */
        JOIN      dbo.io_product_controlrate     AS pcr
                  ON pcr.rcr_idlink_rmr = km.ymr_idlink_armnet AND pcr.rcr_type IN (2, 3) /* Borrower Interest Rate */
        JOIN      dbo.io_product_loanmdt            plm
                  ON plm.rlm_idlink_rmr = km.ymr_idlink_armnet
        JOIN      dbo.io_control_statusmaster    AS css
                  ON css.xsu_id = pm.rmr_idlink_xsu
        LEFT JOIN      (
                      SELECT kpd_idlink_code,
                             CASE
                                 WHEN xtkm_iduser IN (107, 115)
                                     THEN 'Monthly'
                                 WHEN xtkm_iduser IN (114, 116)
                                     THEN 'Fortnightly'
                                 WHEN xtkm_iduser IN (112, 117)
                                     THEN 'Weekly'
                             END             AS payment_frequency,
                             tp.kpd_daystart AS recurring_repayment_day
                        FROM dbo.io_task_pending       AS tp
                        JOIN dbo.io_control_taskmaster AS ict
                             ON xtkm_id = kpd_idlink_xtkm
                                 AND xtkm_iduser IN (107, 112, 114, 115, 116, 117)
                      /*
                      107,Direct Money\Direct Debit\Monthly
                      111,Direct Money\Direct Debit\One Time Only
                      112,Direct Money\Direct Debit\Weekly
                      114,Direct Money\Direct Debit\Fortnightly
                      115,Direct Money\Direct Debit\DD For Recovery\Monthly
                      116,Direct Money\Direct Debit\DD For Recovery\Fortnightly
                      117,Direct Money\Direct Debit\DD For Recovery\Weekly
                      */
                  )                              AS pmt_frequency
                  ON pmt_frequency.kpd_idlink_code = ymr_idlink_armnet
        LEFT JOIN (
                      SELECT kpd_idlink_code,
                             plt.rdd_idlink_cbd,
                             tp.kpd_datenext       AS next_repayment_due_date,
                             plt.rdd_processamount AS repayment_amount
                        FROM dbo.io_task_pending         AS tp
                        JOIN dbo.io_control_taskmaster   AS ict
                             ON xtkm_id = kpd_idlink_xtkm
                                 AND xtkm_iduser IN (107, 112, 114)
                        JOIN dbo.io_product_loantransfer AS plt
                             ON plt.rdd_idlink_kpd = tp.kpd_id
                  )                              AS scheduled_payment
                  ON scheduled_payment.kpd_idlink_code = ymr_idlink_armnet
        LEFT JOIN (
                      SELECT kpd_idlink_code,
                             tp.kpd_datenext       AS adhoc_repayment_date,
                             plt.rdd_processamount AS adhoc_repayment_amount
                        FROM dbo.io_task_pending         AS tp
                        JOIN dbo.io_control_taskmaster   AS ict
                             ON xtkm_id = kpd_idlink_xtkm
                                 AND xtkm_iduser = 111
                        JOIN dbo.io_product_loantransfer AS plt
                             ON plt.rdd_idlink_kpd = tp.kpd_id
                  )                              AS oneoff_payment
                  ON oneoff_payment.kpd_idlink_code = ymr_idlink_armnet
        LEFT JOIN dbo.io_client_bankdetail       AS cbd
                  ON cbd.cbd_id = scheduled_payment.rdd_idlink_cbd
       WHERE ymr_idlink_xfk = '{ef3baea0-8d36-4c5a-8a2d-6ce9e119335b}'
                         )
SELECT *
  FROM arm_loan_final
