import pandas as pd
import numpy as np
import cx_Oracle

def clean_user_data(df):
    df['user_name'] = df['user_name'].astype(str).fillna(np.nan)
    df['customer_zip_code'] = df['customer_zip_code'].astype(str).fillna(np.nan)
    df['customer_city'] = df['customer_city'].fillna(np.nan)
    df['customer_state'] = df['customer_state'].fillna(np.nan)
    return df

def load_data_to_oracle(df, db_connection_string):
    try:
        conn = cx_Oracle.connect(db_connection_string)
        cursor = conn.cursor()
        insert_query = """
            INSERT INTO STG.STG_USERS 
            (USER_NAME, CUSTOMER_ZIP_CODE, CUSTOMER_CITY, CUSTOMER_STATE)
            VALUES 
            (:user_name, :customer_zip_code, :customer_city, :customer_state)
        """
        
        for index, row in df.iterrows():
            cursor.execute(insert_query, {
                'user_name': row['user_name'],
                'customer_zip_code': row['customer_zip_code'],
                'customer_city': row['customer_city'],
                'customer_state': row['customer_state']
            })
        conn.commit()
        print("Data successfully loaded into the database.")

    except cx_Oracle.DatabaseError as e:
        print(f"Error while loading data: {e}")
    
    finally:
        cursor.close()
        conn.close()

def main():
    csv_file = 'ecommerce dataset\\user_dataset.csv'
    df = pd.read_csv(csv_file)
    df_clean = clean_user_data(df)
    db_connection_string = 'system/1212@localhost:1522/xe'
    load_data_to_oracle(df_clean, db_connection_string)

if __name__ == "__main__":
    main()
