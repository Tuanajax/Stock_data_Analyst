import streamlit as st
 
st.title('My First Streamlit App')
st.write("Hello, world!")
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine

# Database credentials (replace with your actual credentials)
DB_USER = 'root'
DB_PASSWORD = 'Eninoskybaby94$' # Or your actual DB password
DB_HOST = 'localhost'
DB_NAME = 'STOCK'


# Create a database connection engine
engine = create_engine(f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}')

st.title("Company Information Dashboard")


@st.cache_data  # Cache the result to speed up subsequent runs
def get_all_symbols():
    try:
        query = "SELECT DISTINCT symbol FROM List_company"
        df_symbols = pd.read_sql(query, con=engine)
        return df_symbols['symbol'].tolist()
    except Exception as e:
        st.error(f"Error fetching symbols: {e}")
        return []


symbols = get_all_symbols()

if symbols:
    selected_symbol = st.selectbox("Select Symbol:", symbols)

    if selected_symbol:
        try:
            # Fetch company profile data
            query = f"SELECT * FROM Company_profile WHERE symbol = '{selected_symbol}'"
            df_profile = pd.read_sql(query, con=engine)

            if not df_profile.empty:
                profile = df_profile.iloc[0].to_dict()  # Convert to dictionary for easier access

                # Display company information
                st.subheader(f"Company Profile: {profile['companyName']}")
                st.write(f"**Symbol:** {profile['symbol']}")

                # Display other relevant information using st.write() or st.markdown()
                st.write(f"**Industry:** {profile.get('industry', 'N/A')}")  # Handle missing fields
                st.write(f"**Website:** {profile.get('website', 'N/A')}")
                st.write(f"**Description:** {profile.get('companyDescription', 'N/A')}")
                # ... add other fields as needed ...


            else:
                st.info("No profile information found for this company.")



        except Exception as e:
            st.error(f"Error: {e}")
else:
    st.warning("No symbols found in the database.")

