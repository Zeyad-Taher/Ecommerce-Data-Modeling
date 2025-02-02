CREATE TABLE DWH.DIM_DATE (
    DATE_ID NUMBER(8) PRIMARY KEY, -- Format MMDDYYYY
    FULL_DATE DATE NOT NULL UNIQUE,
    DAY NUMBER(2) NOT NULL, 
    MONTH NUMBER(2) NOT NULL, 
    YEAR NUMBER(4) NOT NULL, 
    QUARTER NUMBER(1) NOT NULL, 
    DAY_OF_WEEK NUMBER(1) NOT NULL, 
    WEEK_OF_YEAR NUMBER(2), 
    IS_WEEKEND CHAR(1) CHECK (IS_WEEKEND IN ('Y', 'N')), 
    IS_HOLIDAY CHAR(1) CHECK (IS_HOLIDAY IN ('Y', 'N'))
);

CREATE TABLE DWH.DIM_CUSTOMERS (
    CUSTOMER_ID VARCHAR2(50) PRIMARY KEY,
    CUSTOMER_ZIP_CODE VARCHAR2(15),
    CUSTOMER_CITY VARCHAR2(100),
    CUSTOMER_STATE VARCHAR2(100)
);

CREATE TABLE DWH.DIM_PRODUCTS (
    PRODUCT_ID VARCHAR2(50) PRIMARY KEY,
    PRODUCT_CATEGORY VARCHAR2(100),
    PRODUCT_NAME_LENGTH NUMBER(10,0),
    PRODUCT_DESCRIPTION_LENGTH NUMBER(10,0),
    PRODUCT_PHOTOS_QTY NUMBER(10,0),
    PRODUCT_WEIGHT_G NUMBER(15,0),
    PRODUCT_LENGTH_CM NUMBER(10,0),
    PRODUCT_HEIGHT_CM NUMBER(10,0),
    PRODUCT_WIDTH_CM NUMBER(10,0)
);

CREATE TABLE DWH.DIM_SELLERS (
    SELLER_ID VARCHAR2(50) PRIMARY KEY,
    SELLER_ZIP_CODE VARCHAR2(15),
    SELLER_CITY VARCHAR2(100),
    SELLER_STATE VARCHAR2(100)
);

CREATE TABLE DWH.FCT_ORDERS (
    ORDER_ID VARCHAR2(50) PRIMARY KEY,
    CUSTOMER_ID VARCHAR2(50) NOT NULL,
    ORDER_DATE_ID NUMBER(8) NOT NULL,
    ORDER_HOUR NUMBER(2),
    APPROVAL_DATE_ID NUMBER(8),
    PICKUP_DATE_ID NUMBER(8), 
    DELIVERY_DATE_ID NUMBER(8),
    ESTIMATED_DELIVERY_DATE_ID NUMBER(8),
    DELIVERY_DELAY_DAYS NUMBER(5,2),
    SPENDING_TIME NUMBER(5,2),
    ORDER_STATUS VARCHAR2(50),
    FOREIGN KEY (CUSTOMER_ID) REFERENCES DWH.DIM_CUSTOMERS(CUSTOMER_ID),
    FOREIGN KEY (ORDER_DATE_ID) REFERENCES DWH.DIM_DATE(DATE_ID),
    FOREIGN KEY (APPROVAL_DATE_ID) REFERENCES DWH.DIM_DATE(DATE_ID),
    FOREIGN KEY (PICKUP_DATE_ID) REFERENCES DWH.DIM_DATE(DATE_ID),
    FOREIGN KEY (DELIVERY_DATE_ID) REFERENCES DWH.DIM_DATE(DATE_ID),
    FOREIGN KEY (ESTIMATED_DELIVERY_DATE_ID) REFERENCES DWH.DIM_DATE(DATE_ID)
);


CREATE TABLE DWH.FCT_ORDER_ITEMS (
    ORDER_ITEM_ID NUMBER(10,0),
    ORDER_ID VARCHAR2(50) NOT NULL,     -- Foreign Key to FCT_ORDERS
    PRODUCT_ID VARCHAR2(50) NOT NULL,   -- Foreign Key to DIM_PRODUCTS
    SELLER_ID VARCHAR2(50) NOT NULL,    -- Foreign Key to DIM_SELLERS
    PRICE NUMBER(15,4),
    SHIPPING_COST NUMBER(15,4),
    PRIMARY KEY (ORDER_ID, ORDER_ITEM_ID), -- Composite Primary Key
    FOREIGN KEY (ORDER_ID) REFERENCES DWH.FCT_ORDERS(ORDER_ID),
    FOREIGN KEY (PRODUCT_ID) REFERENCES DWH.DIM_PRODUCTS(PRODUCT_ID),
    FOREIGN KEY (SELLER_ID) REFERENCES DWH.DIM_SELLERS(SELLER_ID)
);


CREATE TABLE DWH.FCT_PAYMENTS (
    PAYMENT_ID NUMBER(10,0) PRIMARY KEY,
    ORDER_ID VARCHAR2(50) NOT NULL,
    PAYMENT_SEQUENTIAL NUMBER(10,0),
    PAYMENT_TYPE VARCHAR2(50),
    PAYMENT_INSTALLMENTS NUMBER(10,0),
    PAYMENT_VALUE NUMBER(15,4),
    FOREIGN KEY (ORDER_ID) REFERENCES DWH.FCT_ORDERS(ORDER_ID)
);


CREATE TABLE DWH.FCT_FEEDBACK (
    FEEDBACK_ID VARCHAR2(50),
    ORDER_ID VARCHAR2(50) NOT NULL,
    FEEDBACK_SCORE NUMBER(3,0),
    FEEDBACK_FORM_SENT_DATE_ID NUMBER(8) NOT NULL,
    FEEDBACK_ANSWER_DATE_ID NUMBER(8),
    PRIMARY KEY (FEEDBACK_ID, ORDER_ID),  -- Composite primary key
    FOREIGN KEY (ORDER_ID) REFERENCES DWH.FCT_ORDERS(ORDER_ID),
    FOREIGN KEY (FEEDBACK_FORM_SENT_DATE_ID) REFERENCES DWH.DIM_DATE(DATE_ID),
    FOREIGN KEY (FEEDBACK_ANSWER_DATE_ID) REFERENCES DWH.DIM_DATE(DATE_ID)
);
