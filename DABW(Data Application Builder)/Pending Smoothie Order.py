# Import python packages.
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col

# Write directly to the app.
st.title(f":cup_with_straw: Pending Smoothie Orders :cup_with_straw:")
st.write(
  """
  Orders that need to be filled.
  """
)

#st.write("You selected:", option)

session = get_active_session()

# 1. FIlter the orders table for which order is not filled
my_dataframe = session.table("smoothies.public.orders").filter(col('ORDER_FILLED') == 0) \
                                                        .collect()
                                                       
#st.dataframe(data=my_dataframe, use_container_width=True)

if my_dataframe:
    #2. Convert Dataframe to a Data Editor which returns pandas df
    editable_df = st.data_editor(my_dataframe)
    #st.write(editable_df.columns)

    #create a submit button
    submitted = st.button("Submit")

#If submit button is clicked display success message
    if submitted:
        #3. Convert edited Pandas DF to Snowpark df
        edited_dataset = session.create_dataframe(editable_df)

        #4. Convert edited DataFrame into a temporary table:
        edited_dataset.write.save_as_table("TEMP_ORDER_UPDATES", mode="overwrite")
    
        try:
            session.sql("""
            MERGE INTO smoothies.public.orders AS t
            USING TEMP_ORDER_UPDATES AS s
            ON t.order_uid = s.order_uid
            WHEN MATCHED THEN
                UPDATE SET t.order_filled = s.order_filled
             """).collect()
    
            st.success('Order(s) Updated!', icon="👍")
        
            #drop the temp table finally
            session.sql("DROP TABLE TEMP_ORDER_UPDATES").collect()
        except:
            st.write('Something went wrong!')
else:
    st.success('There are no pending orders right now!', icon="👍")







                 





