import pandas as pd
import cx_Oracle

def extract_products_data(csv_file):
    df = pd.read_csv(csv_file)
    for col in df.columns:
        if df[col].dtype == 'float64':
            df[col] = df[col].where(df[col].notna(), 0) 
            df[col] = pd.to_numeric(df[col], downcast='integer', errors='coerce') 
    df['product_id'] = df['product_id'].astype(str)
    df['product_category'] = df['product_category'].astype(str) 
    return df

def load_products_data_to_oracle(df, db_connection_string):
    try:
        conn = cx_Oracle.connect(db_connection_string)
        cursor = conn.cursor()
        
        insert_query = """
            INSERT INTO STG.STG_PRODUCTS 
            (PRODUCT_ID, PRODUCT_CATEGORY, PRODUCT_NAME_LENGTH, PRODUCT_DESCRIPTION_LENGTH, 
             PRODUCT_PHOTOS_QTY, PRODUCT_WEIGHT_G, PRODUCT_LENGTH_CM, PRODUCT_HEIGHT_CM, PRODUCT_WIDTH_CM)
            VALUES 
            (:product_id, :product_category, :product_name_length, :product_description_length, 
             :product_photos_qty, :product_weight_g, :product_length_cm, :product_height_cm, :product_width_cm)
        """
        
        for index, row in df.iterrows():
            try:
                cursor.execute(insert_query, {
                    'product_id': row['product_id'],
                    'product_category': row['product_category'],
                    'product_name_length': int(row['product_name_lenght']), 
                    'product_description_length': int(row['product_description_lenght']), 
                    'product_photos_qty': int(row['product_photos_qty']), 
                    'product_weight_g': int(row['product_weight_g']), 
                    'product_length_cm': int(row['product_length_cm']),  
                    'product_height_cm': int(row['product_height_cm']),  
                    'product_width_cm': int(row['product_width_cm'])   
                })
            except Exception as e:
                print(f"Error inserting row {index}: {e}")
        
        conn.commit()
        print("Product data loaded successfully.")
    
    except cx_Oracle.DatabaseError as e:
        print(f"Error while loading data: {e}")
    
    finally:
        cursor.close()
        conn.close()

def main():
    csv_file = 'ecommerce dataset/products_dataset.csv' 
    db_connection_string = 'system/1212@localhost:1522/xe'
    df = extract_products_data(csv_file)
    load_products_data_to_oracle(df, db_connection_string)

if __name__ == "__main__":
    main()
