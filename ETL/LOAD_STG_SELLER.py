import pandas as pd
import numpy as np
import cx_Oracle

def clean_seller_data(df):
    df['seller_id'] = df['seller_id'].astype(str).fillna(np.nan)
    df['seller_zip_code'] = df['seller_zip_code'].astype(str).fillna(np.nan)
    df['seller_city'] = df['seller_city'].fillna(np.nan)
    df['seller_state'] = df['seller_state'].fillna(np.nan)
    return df

def load_data_to_oracle(df, db_connection_string):
    try:
        conn = cx_Oracle.connect(db_connection_string)
        cursor = conn.cursor()

        insert_query = """
            INSERT INTO STG.STG_SELLERS 
            (SELLER_ID, SELLER_ZIP_CODE, SELLER_CITY, SELLER_STATE)
            VALUES 
            (:seller_id, :seller_zip_code, :seller_city, :seller_state)
        """
        
        for index, row in df.iterrows():
            cursor.execute(insert_query, {
                'seller_id': row['seller_id'],
                'seller_zip_code': row['seller_zip_code'],
                'seller_city': row['seller_city'],
                'seller_state': row['seller_state']
            })

        conn.commit()
        print("Data successfully loaded into the database.")

    except cx_Oracle.DatabaseError as e:
        print(f"Error while loading data: {e}")
    
    finally:
        cursor.close()
        conn.close()


def main():
    csv_file = 'ecommerce dataset\\seller_dataset.csv' 
    df = pd.read_csv(csv_file)
    df_clean = clean_seller_data(df)
    db_connection_string = 'system/1212@localhost:1522/xe'

    load_data_to_oracle(df_clean, db_connection_string)

if __name__ == "__main__":
    main()
