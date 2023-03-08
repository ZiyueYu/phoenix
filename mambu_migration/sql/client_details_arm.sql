WITH
    arm_cust_final AS (
        SELECT CAST(km.ymr_idlink_foreign AS INT) AS loanid,
               ct.cti_firstname                   AS firstname,
               ct.cti_middlename                  AS middlename,
               ct.cti_surname                     AS surname,
               CAST(cti_dob AS DATE)              AS birthdate,
               ccp.ccd_details                    AS mobile,
               cce.ccd_details                    AS email,
               cad_streetname                     AS line1,
               cad_city                           AS city,
               cad_state                          AS region,
               'Australia'                        AS country,
               cad_postcode                       AS postcode,
               rmr_vedascore                      AS veda_score,
               icct.xcti_detail                   AS title
        FROM dbo.io_keys_masterreference         AS km
        JOIN      dbo.io_product_masterreference AS pm
                  ON pm.rmr_id = km.ymr_idlink_armnet
        JOIN      dbo.io_link_masterreference    AS lm
                  ON lm.lmr_idlink_code_id = km.ymr_idlink_armnet
        JOIN      dbo.io_client_masterreference  AS cm
                  ON cm.cmr_id = lm.lmr_idlink_cmr
        JOIN      dbo.io_client_typeindividual   AS ct
                  ON ct.cti_idlink_cmr = cm.cmr_id
        JOIN      dbo.io_client_addressdetail    AS ca
                  ON ca.cad_idlink_cmr = cm.cmr_id
        JOIN      dbo.io_client_contactdetail    AS ccp
                  ON ccp.ccd_idlink_cmr = cm.cmr_id AND
                     ccp.ccd_idlink_xct =
                     '{29411831-e939-4357-940a-44f55b4a5c9b}' /* Mobile Number*/
        JOIN      dbo.io_client_contactdetail    AS cce
                  ON cce.ccd_idlink_cmr = cm.cmr_id AND
                     cce.ccd_idlink_xct =
                     '{f8d0cbcc-ad81-4ed2-a4eb-ec39ac35168c}'/* Email*/
        LEFT JOIN dbo.io_control_clientgender    AS iccg
                  ON iccg.xcgn_id = ct.cti_idlink_xcgn
        LEFT JOIN dbo.io_control_clienttitle     AS icct
                  ON icct.xcti_id = ct.cti_idlink_xct /* ARM seems to uses a wrong field to link client to its title*/
        WHERE ymr_idlink_xfk =
              '{ef3baea0-8d36-4c5a-8a2d-6ce9e119335b}'
    )
SELECT *
FROM arm_cust_final

