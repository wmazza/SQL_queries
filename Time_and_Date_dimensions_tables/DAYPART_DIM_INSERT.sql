-- DAYPART_DIM
INSERT INTO adobe_ls.DAYPART_DIM
(
     DAYPART_NAME 
    ,DAYPART_START_TIME 
    ,DAYPART_END_TIME 
    ,DAYPART_DIM_ETL_UPDATED_BY 
)
VALUES
    ('MORNING (5a-12p)','5:00:00','11:59:59',''),
    ('DAYSIDE (12p-6p)','12:00:00','17:59:59','m'),
    ('NIGHTSIDE (6p-12a)','18:00:00','23:59:59',''),
    ('OFF (12a-5a)', '0:00:00','4:59:59','');