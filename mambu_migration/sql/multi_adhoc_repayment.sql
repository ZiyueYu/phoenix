SELECT ymr_idlink_foreign,
       count(distinct tp.kpd_id) as count_of_pending
  FROM dbo.io_keys_masterreference    AS km
  LEFT JOIN dbo.io_task_pending       AS tp
            ON tp.kpd_idlink_code = km.ymr_idlink_armnet
  JOIN      dbo.io_control_taskmaster AS ict
            ON xtkm_id = kpd_idlink_xtkm
                AND xtkm_iduser =111
 WHERE ymr_idlink_xfk = '{ef3baea0-8d36-4c5a-8a2d-6ce9e119335b}'
 group by ymr_idlink_foreign
having count(distinct tp.kpd_id) > 1