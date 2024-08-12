import requests
import pandas as pd
import sqlite3
import streamlit as st
from apscheduler.schedulers.background import BackgroundScheduler
import datetime

DATABASE = 'hackathons.db'
BASE_URL = "https://devpost.com/api/hackathons?page={}"

def create_table():
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS hackathons (
        Title TEXT,
        Displayed_Location TEXT,
        Open_State TEXT,
        Analytics_Identifier TEXT,
        Url TEXT PRIMARY KEY,
        Submission_Period_Dates TEXT,
        Themes TEXT,
        Prize_Amount TEXT,
        Registrations_Count INTEGER,
        Featured TEXT,
        Organization_Name TEXT,
        Winners_Announced TEXT,
        Submission_Gallery_Url TEXT,
        Start_A_Submission_Url TEXT,
        Invite_Only TEXT,
        Eligibility_Requirement_Invite_Only_Description TEXT
    )
    ''')
    conn.commit()
    conn.close()

def scrape_hackathons():
    print(f"Starting data scrape at {datetime.datetime.now()}")
    page = 1
    number_of_hackathons = 0
    total_count = 50000  # Initialize with a large number for the loop condition
    
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()

    create_table()

    while True:
        response = requests.get(BASE_URL.format(page))
        data = response.json()
        total_count = data["meta"]["total_count"]
        hackathons = data['hackathons']
        
        if not hackathons:
            break

        new_rows = []
        for item in hackathons:
            number_of_hackathons += 1
            row = {
                "Title": item["title"],
                "Displayed Location": item["displayed_location"],
                "Open State": item['open_state'],
                "Analytics Identifier": item["analytics_identifier"],
                "Url": item['url'],
                "Submission Period Dates": item['submission_period_dates'],
                "Themes": item['themes'],
                "Prize Amount": item['prize_amount'],
                "Registrations Count": item['registrations_count'],
                "Featured": item['featured'],
                "Organization Name": item['organization_name'],
                "Winners Announced": item['winners_announced'],
                "Submission Gallery Url": item['submission_gallery_url'],
                "Start A Submission Url": item['start_a_submission_url'],
                "Invite Only": item['invite_only'],
                "Eligibility Requirement Invite Only Description": item['eligibility_requirement_invite_only_description']
            }
            
            # Check if URL already exists in the database
            cursor.execute("SELECT 1 FROM hackathons WHERE Url = ?", (row['Url'],))
            if cursor.fetchone():
                print(f"Existing row found for URL: {row['Url']}. Stopping scrape.")
                conn.close()
                return
            
            new_rows.append(row)
        
        if new_rows:
            pd.DataFrame(new_rows).to_sql('hackathons', conn, if_exists='append', index=False)
        
        page += 1

    conn.close()
    print(f'Scraped {number_of_hackathons} hackathons. Data stored in SQLite database.')

def load_data():
    conn = sqlite3.connect(DATABASE)
    df = pd.read_sql_query("SELECT * FROM hackathons", conn)
    conn.close()
    return df

def main():
    st.title('Hackathons Dashboard')

    df = load_data()

    if not df.empty:
        st.write(df.head(50))

        csv = df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="Download data as CSV",
            data=csv,
            file_name='hackathons.csv',
            mime='text/csv'
        )
    else:
        st.write("No data available")

    if st.button('Scrape Data'):
        scrape_hackathons()
        st.experimental_rerun()
        #

if __name__ == "__main__":
    # Setup scheduler
    scheduler = BackgroundScheduler()
    scheduler.add_job(scrape_hackathons, 'interval', hours=24)  # Runs daily
    scheduler.start()

    # Run Streamlit app
    main()
