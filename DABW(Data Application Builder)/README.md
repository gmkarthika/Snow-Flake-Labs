The Fruit Smoothie App is a Streamlit application built directly inside Snowflake.

https://melaniessmoothies-2c5mevjft874ewdafas9ph.streamlit.app/

The app simulates a smoothie shop where users can:

✔️ Browse fruits Pulled from a Snowflake table (e.g., smoothies.public.fruits).

✔️ Build a smoothie Users select ingredients from dropdowns.

✔️ Submit an order The app writes the order into a Snowflake table (smoothies.public.orders).

✔️ View order history The app reads from Snowflake and displays past orders.

✔️ Update order status Mark orders as filled / unfilled.

✔️ Use Snowflake features Like UDFs, hashing, secure views, and Streamlit widgets.
