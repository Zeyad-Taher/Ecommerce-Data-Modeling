INSERT INTO DWH.FCT_FEEDBACK (FEEDBACK_ID, ORDER_ID, FEEDBACK_SCORE, FEEDBACK_FORM_SENT_DATE_ID, FEEDBACK_ANSWER_DATE_ID)
SELECT 
    f.FEEDBACK_ID,
    f.ORDER_ID,
    f.FEEDBACK_SCORE,
    d1.DATE_ID AS FEEDBACK_FORM_SENT_DATE_ID,
    d2.DATE_ID AS FEEDBACK_ANSWER_DATE_ID
FROM 
    STG.STG_FEEDBACK f
LEFT JOIN DWH.DIM_DATE d1 ON TRUNC(f.FEEDBACK_FORM_SENT_DATE) = d1.FULL_DATE
LEFT JOIN DWH.DIM_DATE d2 ON TRUNC(f.FEEDBACK_ANSWER_DATE) = d2.FULL_DATE;