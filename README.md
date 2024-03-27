# Fetch-Data-Analysis

Greetings and welcome to my Fetch Demo code repo!

First up is the basic_db_diagram.png.  This is a crude diagram of what columns can be joined between the three json files.  I would create three different dataframes containing all of the data with the json files.  The schema remains the same as described here:
https://fetch-hiring.s3.amazonaws.com/analytics-engineer/ineeddata-data-modeling/data-modeling.html

Receipts can join to Users by joining UserId from Receipts to _id from Users.  And Brands can join to Receipts by pulling out barcode or name from the rewardsReceiptItemList.  Join to barcode in Brands easily enough.  Additional text cleaning/processing is required to join to the name column in Brands.

Next is stable_builder_final.py which is a python script that creates dataframes from three json files and performs some rudimentary analysis on the data.

Run the script in the commandline as follows:
python3 table_builder.py receipts.json users.json brands.json

For a few questions and concerns about data quality, see the data_quality_issues.txt file.

And for a message with questions for the stakeholders, please see the stakeholder_comminque.tx file.

Thanks and enjoy!
