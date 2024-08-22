# Import python packages
import streamlit as st
from snowflake.snowpark.functions import col, when_matched

# Write directly to the app
st.title(":cup_with_straw: Pending Smoothie Orders :cup_with_straw:")
st.write("""Order that need to be filled""")

# Get the current credentials
cnx = st.connection('snowflake')
session = cnx.session()
my_dataframe = session.table("smoothies.public.orders").filter(col("ORDER_FILLED")==0).collect()

if my_dataframe:
  editable_df = st.data_editor(my_dataframe)  
  submitted = st.button("Order Completed") 
  if submitted:
      og_dataset = session.table("smoothies.public.orders")
      edited_dataset = session.create_dataframe(editable_df)
      try:
           og_dataset.merge(edited_dataset
                         , (og_dataset['ORDER_UID'] == edited_dataset['ORDER_UID'])
                         , [when_matched().update({'ORDER_FILLED': edited_dataset['ORDER_FILLED']})]
                        )
           st.success('Order Successfully Completed', icon='👍')
           st.experimental_rerun()
      except:
          st.write("Something went wrong.")0
else:
    st.success("There is no pending orders right now", icon='👍')
        
       
