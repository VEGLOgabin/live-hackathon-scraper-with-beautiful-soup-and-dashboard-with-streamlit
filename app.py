import requests
import pandas as pd
import sqlite3
import streamlit as st
from apscheduler.schedulers.background import BackgroundScheduler
import datetime
import re
import plotly.express as px


DATABASE = 'hackathons.db'
BASE_URL = "https://devpost.com/api/hackathons?page={}"

def create_table():
    conn = None
    try:
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
            Eligibility_Requirement_Invite_Only_Description TEXT DEFAULT NULL
        )
        ''')
        conn.commit()
    except sqlite3.Error as e:
        print(f"Database error: {e}")
    finally:
        if conn:
            conn.close()

def check_table_exists():
    conn = None
    exists = False
    try:
        conn = sqlite3.connect(DATABASE)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='hackathons'")
        exists = cursor.fetchone() is not None
    except sqlite3.Error as e:
        print(f"Database error: {e}")
    finally:
        if conn:
            conn.close()
    return exists

def scrape_hackathons():
    print(f"Starting data scrape at {datetime.datetime.now()}")
    page = 1
    number_of_hackathons = 0

    conn = None
    cursor = None

    try:
        conn = sqlite3.connect(DATABASE)
        cursor = conn.cursor()

        create_table()  # Ensure the table is created

        while True:
            try:
                response = requests.get(BASE_URL.format(page))
                response.raise_for_status()  # Raise an error for bad status codes
                data = response.json()
            except requests.RequestException as e:
                print(f"Request error: {e}")
                break
            except ValueError as e:
                print(f"JSON decode error: {e}")
                print(f"Response text: {response.text}")
                break

            hackathons = data.get('hackathons', [])

            if not hackathons:
                break

            for item in hackathons:
                number_of_hackathons += 1
                row = {
                    "Title": item.get("title", ""),
                    "Displayed_Location": item.get("displayed_location", {}).get("location", ""),
                    "Open_State": item.get('open_state', ""),
                    "Analytics_Identifier": item.get("analytics_identifier", ""),
                    "Url": item.get('url', ""),
                    "Submission_Period_Dates": item.get('submission_period_dates', ""),
                    "Themes": item.get('themes', []),
                    "Prize_Amount": item.get('prize_amount', ""),
                    "Registrations_Count": item.get('registrations_count', 0),
                    "Featured": item.get('featured', ""),
                    "Organization_Name": item.get('organization_name', ""),
                    "Winners_Announced": item.get('winners_announced', ""),
                    "Submission_Gallery_Url": item.get('submission_gallery_url', ""),
                    "Start_A_Submission_Url": item.get('start_a_submission_url', ""),
                    "Invite_Only": item.get('invite_only', ""),
                    "Eligibility_Requirement_Invite_Only_Description": item.get('eligibility_requirement_invite_only_description', "")
                }

                try:
                    cursor.execute("SELECT 1 FROM hackathons WHERE Url = ?", (row['Url'],))
                    if cursor.fetchone():
                        print(f"Existing row found for URL: {row['Url']}. Skipping insertion.")
                    else:
                        row['Themes'] = ', '.join(theme['name'] for theme in row['Themes'])
                        match = re.search(r'>([\d,]+)<', row["Prize_Amount"])
                        if match:
                            amount = match.group(1).replace(',', '')
                            row["Prize_Amount"] = amount
                        else:
                            print(f"No valid amount found in prize data: {row['Prize_Amount']}")

                        cursor.execute('''
                        INSERT INTO hackathons (
                            Title, Displayed_Location, Open_State, Analytics_Identifier, Url,
                            Submission_Period_Dates, Themes, Prize_Amount, Registrations_Count,
                            Featured, Organization_Name, Winners_Announced, Submission_Gallery_Url,
                            Start_A_Submission_Url, Invite_Only, Eligibility_Requirement_Invite_Only_Description
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            row["Title"], row["Displayed_Location"], row["Open_State"], row["Analytics_Identifier"], row["Url"],
                            row["Submission_Period_Dates"], row["Themes"], row["Prize_Amount"], row["Registrations_Count"],
                            row["Featured"], row["Organization_Name"], row["Winners_Announced"], row["Submission_Gallery_Url"],
                            row["Start_A_Submission_Url"], row["Invite_Only"], row["Eligibility_Requirement_Invite_Only_Description"]
                        ))
                        conn.commit()
                        print(f"Inserted new row for URL: {row['Url']}")

                except sqlite3.Error as e:
                    print(f"Database error during insert: {e}")

            page += 1

    except sqlite3.Error as e:
        print(f"Database error: {e}")
    finally:
        if conn:
            conn.close()

    print(f'Scraped {number_of_hackathons} hackathons. Data stored in SQLite database.')

def load_data():
    conn = None
    df = pd.DataFrame()
    try:
        if not check_table_exists():
            print("Table 'hackathons' does not exist. Skipping data load.")
            return df
        
        conn = sqlite3.connect(DATABASE)
        df = pd.read_sql_query("SELECT * FROM hackathons", conn)
    except sqlite3.Error as e:
        print(f"Database error: {e}")
    finally:
        if conn:
            conn.close()
    return df

def main():
    # ---- HIDE STREAMLIT STYLE ----
    hide_st_style = """
                <style>
                #MainMenu {visibility: hidden;}
                footer {visibility: hidden;}
                header {visibility: hidden;}
                </style>
                """
    st.markdown(hide_st_style, unsafe_allow_html=True)
    
    st.title('Hackathons Dashboard')
    
    

    df = load_data()
    

    if not df.empty:
        # Filter options
        # col1, col2 = st.columns([2, 1])
        # with col1:
        #     pass
        # with col2:
        #     csv = filtered_df.to_csv(index=False).encode('utf-8')
        #     st.download_button(
        #         label="Download data as CSV",
        #         data=csv,
        #         file_name='hackathons.csv',
        #         mime='text/csv'
        #     )
            
            
        st.sidebar.header('Filter Options')
      
        open_state = st.sidebar.multiselect(
            'Open State',
            options=df['Open_State'].unique(),
            default=df['Open_State'].unique()
        )
        
        displayed_location = st.sidebar.multiselect(
            'Displayed Location',
            options=df['Displayed_Location'].unique(),
            default=df['Displayed_Location'].unique()
        )
        themes = st.sidebar.multiselect(
            'Topics',
            options=df['Themes'].str.split(', ').explode().unique(),
            default=df['Themes'].str.split(', ').explode().unique()
        )
        
     
        
        filtered_df = df[
            df['Open_State'].isin(open_state) &
            df['Displayed_Location'].isin(displayed_location) &
            df['Themes'].str.contains('|'.join(themes), na=False)
        ]
        
        st.divider()



        # Display apply links for open and upcoming hackathons
        st.subheader('Apply Links for Open and Upcoming Hackathons')
        show_hackathons = st.checkbox("Show upcoming and open hackathons to apply", key="One")
        if show_hackathons:
            
            for index, row in filtered_df[filtered_df['Open_State'].isin(['open', 'upcoming'])].iterrows():
                title_col, link_col = st.columns([2, 1])
                with title_col:
                    st.subheader(row['Title'])
                    st.write(f"Location: {row['Displayed_Location']}")
                with link_col:
                    
                    st.link_button("Apply", row['Url'])
                    
                st.divider()

            

        # Pie chart for Open State statistics
        st.subheader('Open State Statistics')
        state_counts = filtered_df['Open_State'].value_counts().reset_index()
        state_counts.columns = ['Open State', 'Count']
        fig = px.pie(state_counts, names='Open State', values='Count', title='Hackathons by Open State')
        st.plotly_chart(fig)
        
        st.divider()

        # Statistics dashboard
        st.subheader('Statistics Dashboard')
        total_rows = filtered_df.shape[0]
        open_count = filtered_df[filtered_df['Open_State'] == 'open'].shape[0]
        upcoming_count = filtered_df[filtered_df['Open_State'] == 'upcoming'].shape[0]
        ended_count = filtered_df[filtered_df['Open_State'] == 'ended'].shape[0]

        st.write(f"Total Rows: {total_rows}")
        st.write(f"Open: {open_count}")
        st.write(f"Upcoming: {upcoming_count}")
        st.write(f"Ended: {ended_count}")
        
        st.divider()

        # Create histogram
        fig = px.histogram(filtered_df, x='Open_State', title='Distribution of Open States')
        st.plotly_chart(fig)
        
        st.divider()
        
        # Show pandas data frame session
        st.subheader('View the first fifty rows of the data')
        show_table = st.checkbox("Show your first fifty filtered data rows as a table", key="Two")
        if show_table:
            st.table(filtered_df.head(50))
        
        st.divider()
        
        csv = filtered_df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="Download data as CSV",
            data=csv,
            file_name='hackathons.csv',
            mime='text/csv'
        )
        
        st.divider() # Used to draw a horizontal line to separate 
        
        st.snow()
        # st.balloons()
    else:
        st.write("No data available")

    if st.button('Scrape Data'):
        try:
            scrape_hackathons()
        except Exception as e:
            st.error(f"An error occurred during scraping: {e}")
        st.session_state.refresh = True

if __name__ == "__main__":
    # Setup scheduler
    scheduler = BackgroundScheduler()
    scheduler.add_job(scrape_hackathons, 'interval', hours=24)  # Runs daily
    scheduler.start()

    # Run Streamlit app
    main()
