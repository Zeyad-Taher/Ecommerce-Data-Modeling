import pandas as pd
import cx_Oracle

def extract_orders_data(csv_file):
    df = pd.read_csv(csv_file)
    
    date_columns = ['order_date', 'order_approved_date', 'pickup_date', 'delivered_date', 'estimated_time_delivery']
    for col in date_columns:
        df[col] = pd.to_datetime(df[col], errors='coerce')
    
    for col in df.columns:
        df[col] = df[col].apply(lambda x: None if pd.isna(x) or x == '' else x)
    df['order_id'] = df['order_id'].astype(str)
    df['user_name'] = df['user_name'].astype(str)
    df['order_status'] = df['order_status'].astype(str)
    return df

def load_orders_data_to_oracle(df, db_connection_string):
    try:
        conn = cx_Oracle.connect(db_connection_string)
        cursor = conn.cursor()

        cursor.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'")
        
        insert_query = """
            INSERT INTO STG.STG_ORDERS 
            (ORDER_ID, USER_NAME, ORDER_STATUS, ORDER_DATE, ORDER_APPROVED_DATE, 
             PICKUP_DATE, DELIVERED_DATE, ESTIMATED_TIME_DELIVERY)
            VALUES 
            (:order_id, :user_name, :order_status, :order_date, :order_approved_date,
             :pickup_date, :delivered_date, :estimated_time_delivery)
        """
        
        for index, row in df.iterrows():
            order_date = None if pd.isna(row['order_date']) else row['order_date']
            order_approved_date = None if pd.isna(row['order_approved_date']) else row['order_approved_date']
            pickup_date = None if pd.isna(row['pickup_date']) else row['pickup_date']
            delivered_date = None if pd.isna(row['delivered_date']) else row['delivered_date']
            estimated_time_delivery = None if pd.isna(row['estimated_time_delivery']) else row['estimated_time_delivery']
            
            cursor.execute(insert_query, {
                'order_id': row['order_id'],
                'user_name': row['user_name'],
                'order_status': row['order_status'],
                'order_date': order_date,
                'order_approved_date': order_approved_date,
                'pickup_date': pickup_date,
                'delivered_date': delivered_date,
                'estimated_time_delivery': estimated_time_delivery
            })
        
        conn.commit()
        print("Order data loaded successfully.")
    
    except cx_Oracle.DatabaseError as e:
        print(f"Error while loading data: {e}")
    
    finally:
        cursor.close()
        conn.close()

def main():
    csv_file = 'ecommerce dataset\\order_dataset.csv'
    db_connection_string = 'system/1212@localhost:1522/xe'
    df = extract_orders_data(csv_file)
    
    load_orders_data_to_oracle(df, db_connection_string)

if __name__ == "__main__":
    main()
