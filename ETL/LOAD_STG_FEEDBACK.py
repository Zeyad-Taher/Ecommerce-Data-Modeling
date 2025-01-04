import pandas as pd
import cx_Oracle

def extract_feedback_data(csv_file):
    df = pd.read_csv(csv_file)
    
    df['FEEDBACK_FORM_SENT_DATE'] = pd.to_datetime(df['feedback_form_sent_date'], errors='coerce')
    df['FEEDBACK_ANSWER_DATE'] = pd.to_datetime(df['feedback_answer_date'], errors='coerce')
    df['FEEDBACK_SCORE'] = pd.to_numeric(df['feedback_score'], errors='coerce')
    return df


def load_feedback_data_to_oracle(df, db_connection_string):
    try:
        conn = cx_Oracle.connect(db_connection_string)
        cursor = conn.cursor()
        cursor.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'")
        insert_query = """
            INSERT INTO STG.STG_FEEDBACK 
            (FEEDBACK_ID, ORDER_ID, FEEDBACK_SCORE, FEEDBACK_FORM_SENT_DATE, FEEDBACK_ANSWER_DATE)
            VALUES 
            (:feedback_id, :order_id, :feedback_score, :feedback_form_sent_date, :feedback_answer_date)
        """
        
        for index, row in df.iterrows():
            feedback_form_sent_date = None if pd.isna(row['feedback_form_sent_date']) else row['feedback_form_sent_date']
            feedback_answer_date = None if pd.isna(row['feedback_answer_date']) else row['feedback_answer_date']
            cursor.execute(insert_query, {
                'feedback_id': row['feedback_id'],
                'order_id': row['order_id'],
                'feedback_score': int(row['FEEDBACK_SCORE']),
                'feedback_form_sent_date': feedback_form_sent_date,
                'feedback_answer_date': feedback_answer_date
            })
            
        conn.commit()
        print("Feedback data loaded successfully.")
    
    except cx_Oracle.DatabaseError as e:
        print(f"Error while loading data: {e}")
    
    finally:
        cursor.close()
        conn.close()

def main():
    csv_file = 'ecommerce dataset\\feedback_dataset.csv'
    db_connection_string = 'system/1212@localhost:1522/xe'
    df = extract_feedback_data(csv_file)
    load_feedback_data_to_oracle(df, db_connection_string)

if __name__ == "__main__":
    main()
