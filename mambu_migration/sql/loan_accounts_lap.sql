  WITH lap_loan_final AS (
      SELECT wl.loanid,
             IFF(issecuredrateloan = 1, 'Secured', 'Unsecured')  AS rate_type,
             loanpurpose                                         AS loan_purpose,
             IFF(COALESCE(issecureloan, 0) = 1, 'True', 'False') AS is_secure
        FROM history.v_c_lap_dbo_wealthx_loan    AS wl
        LEFT JOIN history.v_c_lap_dbo_tb_loansecurity AS ls
             ON ls.loanid = wl.loanid
                         )
SELECT *
  FROM lap_loan_final
