import pandas as pd
import cx_Oracle

def extract_data(csv_file):
    df = pd.read_csv(csv_file)
    df['PICKUP_LIMIT_DATE'] = pd.to_datetime(df['pickup_limit_date'], errors='coerce')
    df['PRICE'] = pd.to_numeric(df['price'], errors='coerce')
    df['SHIPPING_COST'] = pd.to_numeric(df['shipping_cost'], errors='coerce')
    return df

def load_data_to_oracle(df, db_connection_string):
    try:
        conn = cx_Oracle.connect(db_connection_string)
        cursor = conn.cursor()
        cursor.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'")
        insert_query = """
            INSERT INTO STG.STG_ORDER_ITEMS 
            (ORDER_ID, ORDER_ITEM_ID, PRODUCT_ID, SELLER_ID, PICKUP_LIMIT_DATE, PRICE, SHIPPING_COST) 
            VALUES 
            (:order_id, :order_item_id, :product_id, :seller_id, :pickup_limit_date, :price, :shipping_cost)
        """

        for index, row in df.iterrows():
            pickup_limit_date = None if pd.isna(row['pickup_limit_date']) else row['pickup_limit_date']
            cursor.execute(insert_query, {
                'order_id': row['order_id'],
                'order_item_id': int(row['order_item_id']),
                'product_id': row['product_id'],
                'seller_id': row['seller_id'],
                'pickup_limit_date': pickup_limit_date,
                'price': row['PRICE'],
                'shipping_cost': row['SHIPPING_COST']
            })
        
        conn.commit()
        print("Data loaded successfully.")
    
    except cx_Oracle.DatabaseError as e:
        print(f"Error while loading data: {e}")
    
    finally:
        cursor.close()
        conn.close()

def main():
    csv_file = 'ecommerce dataset\order_item_dataset.csv'
    db_connection_string = 'system/1212@localhost:1522/xe'
    df = extract_data(csv_file)
    
    load_data_to_oracle(df, db_connection_string)

if __name__ == "__main__":
    main()
