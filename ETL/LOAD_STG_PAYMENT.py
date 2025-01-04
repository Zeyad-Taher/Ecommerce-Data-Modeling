import pandas as pd
import cx_Oracle

def extract_payments_data(csv_file):
    df = pd.read_csv(csv_file)
    for col in df.columns:
        df[col] = df[col].where(df[col].notna(), None)
    df['order_id'] = df['order_id'].astype(str)
    df['payment_type'] = df['payment_type'].astype(str)
    df['payment_sequential'] = pd.to_numeric(df['payment_sequential'], errors='coerce')
    df['payment_installments'] = pd.to_numeric(df['payment_installments'], errors='coerce')
    df['payment_value'] = pd.to_numeric(df['payment_value'], errors='coerce')
    return df

def load_payments_data_to_oracle(df, db_connection_string):
    try:
        conn = cx_Oracle.connect(db_connection_string)
        cursor = conn.cursor()
        insert_query = """
            INSERT INTO STG.STG_PAYMENTS 
            (ORDER_ID, PAYMENT_SEQUENTIAL, PAYMENT_TYPE, PAYMENT_INSTALLMENTS, PAYMENT_VALUE)
            VALUES 
            (:order_id, :payment_sequential, :payment_type, :payment_installments, :payment_value)
        """
        
        for index, row in df.iterrows():
            cursor.execute(insert_query, {
                'order_id': row['order_id'],
                'payment_sequential': row['payment_sequential'],
                'payment_type': row['payment_type'],
                'payment_installments': row['payment_installments'],
                'payment_value': row['payment_value']
            })
        
        conn.commit()
        print("Payment data loaded successfully.")
    
    except cx_Oracle.DatabaseError as e:
        print(f"Error while loading data: {e}")
    
    finally:
        cursor.close()
        conn.close()

def main():
    csv_file = 'ecommerce dataset\payment_dataset.csv'
    db_connection_string = 'system/1212@localhost:1522/xe'
    df = extract_payments_data(csv_file)
    
    load_payments_data_to_oracle(df, db_connection_string)

if __name__ == "__main__":
    main()
