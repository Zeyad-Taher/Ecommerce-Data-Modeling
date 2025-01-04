INSERT INTO DWH.FCT_ORDERS (
    ORDER_ID, 
    CUSTOMER_ID, 
    ORDER_DATE_ID,
    ORDER_HOUR,
    APPROVAL_DATE_ID,
    PICKUP_DATE_ID,
    DELIVERY_DATE_ID,
    ESTIMATED_DELIVERY_DATE_ID, 
    DELIVERY_DELAY_DAYS, 
    SPENDING_TIME,
    ORDER_STATUS
)
SELECT 
    ORDER_ID,
    USER_NAME AS CUSTOMER_ID,
    d1.DATE_ID AS ORDER_DATE_ID,
    TO_CHAR(o.ORDER_DATE, 'HH24') AS ORDER_HOUR,
    d2.DATE_ID AS APPROVAL_DATE_ID,
    d3.DATE_ID AS PICKUP_DATE_ID,
    d4.DATE_ID AS DELIVERY_DATE_ID,
    d5.DATE_ID AS ESTIMATED_DELIVERY_DATE_ID, 
    CASE 
        WHEN o.DELIVERED_DATE IS NOT NULL AND o.ESTIMATED_TIME_DELIVERY IS NOT NULL 
        THEN (o.DELIVERED_DATE - o.ESTIMATED_TIME_DELIVERY)
        ELSE NULL
    END AS DELIVERY_DELAY_DAYS,
    CASE 
        WHEN o.DELIVERED_DATE IS NOT NULL AND o.ORDER_DATE IS NOT NULL 
        THEN (o.DELIVERED_DATE - o.ORDER_DATE)
        ELSE NULL
    END AS SPENDING_TIME,
    ORDER_STATUS
FROM (
    SELECT DISTINCT
        o.ORDER_ID,
        o.USER_NAME,
        o.ORDER_DATE,
        o.ORDER_APPROVED_DATE,
        o.PICKUP_DATE,
        o.DELIVERED_DATE,
        o.ESTIMATED_TIME_DELIVERY,
        o.ORDER_STATUS
    FROM 
        STG.STG_ORDERS o
) o
LEFT JOIN DWH.DIM_DATE d1 ON TRUNC(o.ORDER_DATE) = d1.FULL_DATE
LEFT JOIN DWH.DIM_DATE d2 ON TRUNC(o.ORDER_APPROVED_DATE) = d2.FULL_DATE
LEFT JOIN DWH.DIM_DATE d3 ON TRUNC(o.PICKUP_DATE) = d3.FULL_DATE
LEFT JOIN DWH.DIM_DATE d4 ON TRUNC(o.DELIVERED_DATE) = d4.FULL_DATE
LEFT JOIN DWH.DIM_DATE d5 ON TRUNC(o.ESTIMATED_TIME_DELIVERY) = d5.FULL_DATE;
