DECLARE
    start_date DATE := TO_DATE('2010-01-01', 'YYYY-MM-DD');
    end_date DATE := TO_DATE('2030-12-31', 'YYYY-MM-DD');
BEGIN
    FOR d IN (
        SELECT start_date + LEVEL - 1 AS full_date
        FROM DUAL
        CONNECT BY LEVEL <= (end_date - start_date + 1)
    )
    LOOP
        INSERT INTO DWH.DIM_DATE (
            DATE_ID,
            FULL_DATE,
            DAY,
            MONTH,
            YEAR,
            QUARTER,
            DAY_OF_WEEK,
            WEEK_OF_YEAR,
            IS_WEEKEND,
            IS_HOLIDAY
        )
        VALUES (
            TO_NUMBER(TO_CHAR(d.full_date, 'MMDDYYYY')),
            d.full_date,
            EXTRACT(DAY FROM d.full_date),
            EXTRACT(MONTH FROM d.full_date),
            EXTRACT(YEAR FROM d.full_date),
            TO_NUMBER(TO_CHAR(d.full_date, 'Q')),
            TO_NUMBER(TO_CHAR(d.full_date, 'D')),
            TO_NUMBER(TO_CHAR(d.full_date, 'IW')),
            CASE WHEN TO_CHAR(d.full_date, 'D') IN ('1', '7') THEN 'Y' ELSE 'N' END,
            'N'
        );
    END LOOP;

    COMMIT;
END;
