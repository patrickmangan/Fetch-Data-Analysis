import sys
import pandas as pd

# read in the json files provided as arguments
# ex: python3 table_builder.py receipts.json users.json brands.json

input_file = sys.argv[1]

users_file = sys.argv[2]

brands_file = sys.argv[3]

# create a pandas dataframe from each input file

receipts_df = pd.read_json(input_file, lines=True)

users_df = pd.read_json(users_file, lines=True)

brands_df = pd.read_json(brands_file, lines=True)

# explode the rewardsReceiptItemList list so you can access each item individually

exploded_df = receipts_df.explode('rewardsReceiptItemList')

# the following are two functions that access the 'barcode' and 'description' dictionary items from the 'rewardsReceiptItemList' list
# the functions also account for missing or corrupted data with try and except methods

def extract_barcode(item):
    if isinstance(item, dict):
        try:
            return item['barcode']
        except KeyError:
            return None 
    else:
        return None

def extract_description(item):
    if isinstance(item, dict):
        try:
            return item['description']
        except KeyError:
            return None
    else:
        return None

# apply the above functions and create new columns

exploded_df['barcode_values'] = exploded_df['rewardsReceiptItemList'].apply(extract_barcode)

exploded_df['description_values'] = exploded_df['rewardsReceiptItemList'].apply(extract_description)

# create new transaction id column from its nested dictionary structure

exploded_df['transaction_id'] = exploded_df['_id'].apply(lambda x: x["$oid"])

# convert transaction unix date into human readable date

exploded_df['createDate_readable'] = pd.to_datetime(exploded_df['createDate'].apply(lambda x: x['$date']), unit='ms')

# now that the receipts json has been loaded and partially cleaned, we can start answering the provided questions


# 1. What are the top 5 brands by receipts scanned for most recent month?

# filter for only transactionsn in the past month
# the newest transaction date I found in the data is 2021-03-01.  So for I am considering the previous month as the most recent month.
# If given this task by a client/co-worker I would get clarification on the actual date range they are looking for.  March 2021?  Feb 2021?  Past 30 days?

date_filtered_df = exploded_df[exploded_df['createDate_readable'] >= "2021-02-01"]

# select only the required columns for counting the most transactions by brand.  In this case I'm using the 'description_values' (originally 'description' in the receipts.json).
# I could join to brands.json on 'barcode' but that seemed to leave too much out.  With more time I would figure out if I could do text cleaning on the 'description' column and join it to 'name' in brands.json
date_filtered_df_sel = date_filtered_df[['transaction_id','description_values']].drop_duplicates()

# print out top 6 results.  6 because "ITEM NOT FOUND" is one of the values, and that is obviously not a product.  With more time I would investigate these "ITEM NOT FOUND" records.

print(date_filtered_df_sel.groupby(['description_values']).size().sort_values(ascending=False).head(6))

# RESULTS
# description_values
# thindust summer face mask - sun protection neck gaiter for outdooractivities                                                                                   40
# mueller austria hypergrind precision electric spice/coffee grinder millwith large grinding capacity and hd motor also for spices, herbs, nuts,grains, white    40
# ITEM NOT FOUND                                                                                                                                                 32
# flipbelt level terrain waist pouch, neon yellow, large/32-35                                                                                                   28
# DORITOS TORTILLA CHIP SPICY SWEET CHILI REDUCED FAT BAG 1 OZ                                                                                                    3
# JIF CRMY PNT BTR JAR 40 OZ                                                                                                                                      2

# 2. How does the ranking of the top 5 brands by receipts scanned for the recent month compare to the ranking for the previous month?

# filter the same exploded_df for the previous month, in this case January, 2021

prev_date_filtered_df = exploded_df[(exploded_df['createDate_readable'] >= "2021-01-01") & (exploded_df['createDate_readable'] < "2021-02-01")]

# select columns, distinct, run a groupby count

prev_date_filtered_df_sel = prev_date_filtered_df[['transaction_id','description_values']].drop_duplicates()

print(prev_date_filtered_df_sel.groupby(['description_values']).size().sort_values(ascending=False).head(6))

# RESULTS
# description_values
# ITEM NOT FOUND                                                         133
# HUGGIES SIMPLY CLEAN PREMOISTENED WIPE FRAGRANCE FREE BAG 216 COUNT     26
# KLARBRUNN 12PK 12 FL OZ                                                 24
# flipbelt level terrain waist pouch, neon yellow, large/32-35            22
# KLEENEX POP UP RECTANGLE BOX FACIAL TISSUE 2 PLY 8PK 160 CT             21
# Ben & Jerry's Chunky Monkey Non-Dairy Frozen Dessert 16 oz              20

# The January's most common items are wildly different than February's.  I don't know who is buying this much ice cream in the winter...


# 3. When considering average spend from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?

# create new dfs that filter all receipts for the 'rewardsReceiptStatus'.  
# When examining the data, the values I see are "FINISHED" and "REJECTED".  I am assuming that "FINISHED" means "Accepted".

accepted_exp_filtered_df = exploded_df[exploded_df['rewardsReceiptStatus'] == "FINISHED"]
rejected_exp_filtered_df = exploded_df[exploded_df['rewardsReceiptStatus'] == "REJECTED"]

# average the 'totalSpent' column for each filtered df. I think this is the right field to average, though some of the prices look funny to me.
# print the results

accepted_exp_filtered_df_sel = accepted_exp_filtered_df[['transaction_id','totalSpent']].drop_duplicates()
rejected_exp_filtered_df_sel = rejected_exp_filtered_df[['transaction_id','totalSpent']].drop_duplicates()

average_total_spent_accepted = accepted_exp_filtered_df_sel['totalSpent'].astype(float).mean()
average_total_spent_rejected = rejected_exp_filtered_df_sel['totalSpent'].astype(float).mean()

print("the average total spent accepted is " + str(average_total_spent_accepted))
print("the average total spent rejected is " + str(average_total_spent_rejected))

# RESULTS
# the average total spent accepted is 80.85430501930502
# the average total spent rejected is 23.326056338028174
# The average total spend of 'Accepted' receipts is great than 'Rejected' receipts.


# 4. When considering total number of items purchased from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?

# using the filtered dfs above, select the 'transaction_id' and 'purchasedItemCount' columns and get only distinct values.

accepted_purchase_item_count_df = accepted_exp_filtered_df[['transaction_id','purchasedItemCount']].drop_duplicates()
rejected_purchase_item_count_df = rejected_exp_filtered_df[['transaction_id','purchasedItemCount']].drop_duplicates()

# sum the total number of purchased items for all distinct transactions.

total_purchase_item_count_accepted = accepted_purchase_item_count_df['purchasedItemCount'].astype(float).sum()
total_purchase_item_count_rejected = rejected_purchase_item_count_df['purchasedItemCount'].astype(float).sum()

# print the results

print("the total accepted purchase item count is " + str(total_purchase_item_count_accepted))
print("the total rejected purchase item count is " + str(total_purchase_item_count_rejected))

# RESULTS
# the total accepted purchase item count is 8184.0
# the total rejected purchase item count is 173.0


# 5. Which brand has the most spend among users who were created within the past 6 months?

# prepare the users.json data by adding a human readable date:

users_df['createDate_readable'] = pd.to_datetime(users_df['createdDate'].apply(lambda x: x['$date']), unit='ms')

# figure out most recent user created date:
print(users_df.sort_values(by='createDate_readable',ascending=False))

# the most recent date is 2021-02-12 14:11:06.240
# therefore filter from date "2020-09-01" to get only accounts created in the past 6 months

accounts_past_six_months = users_df[users_df['createDate_readable'] >= "2020-09-01"]

# pulling out userId from users.json
# create new column that matches receipts.json column header
# "_id":{"$oid":"5ff1e194b6a9d73a3a9f1052"}

accounts_past_six_months['userId'] = accounts_past_six_months['_id'].apply(lambda x: x["$oid"])

# join my exploded receipts df to the filtered users df on 'userId'
# I used an inner join because I want only receipts that have a userId that we have accounted for in the users.json data.

merged_df = pd.merge(exploded_df, accounts_past_six_months, on='userId', how='inner')

# at this point I realized I hadn't really used the brands.json data at all.  So I am going to join it the the users and receipts dfs to answer the last two questions
# first need to cast the barcode value to a string and rename it to merge with the exploded receipts df.

brands_df['barcode_values'] = brands_df['barcode'].astype(str)

# join the brands df to the merged users and receipts df on 'barcode_values'.  Do a left hand join, because not many barcodes actually join from brands.json
# This way I account for all transactions and can do a sum/count on either the brand name in brands.json or the description field in receipts.json

merged_barcodes_df = pd.merge(merged_df, brands_df, on='barcode_values', how='left')

# let's select the most important columns and do a distinct

merged_barcodes_df_sel = merged_barcodes_df[['userId','barcode_values','description_values','totalSpent','createDate_readable_x','active','barcode','category','name']].drop_duplicates()

# at this point I wanted to look at how many barcodes actually joined, so I wrote the data out into a csv file and looked at it in google sheets
# comment in if you want to write the data out to a csv
# csv_file_path = 'merged_barcodes_data.csv'
# merged_barcodes_df_sel.to_csv(csv_file_path, index=False)

# groupby the brand name and take the some of the 'totalSpent' values
grouped_df = merged_barcodes_df_sel.groupby('name')['totalSpent'].sum().reset_index().sort_values('totalSpent',ascending=False)

# print and sort the results
print(grouped_df)

# RESULTS
#                      name  totalSpent
# 10               Tostitos     7527.79
# 9                 Swanson     7187.14
# 1   Cracker Barrel Cheese     4885.89
# 4                  Jell-O     4754.37
# 0                 Cheetos     4721.95
# 2         Diet Chris Cola     4721.95
# 6         Pepperidge Farm     4721.95
# 7                   Prego     4721.95
# 11                     V8     4721.95
# 5            Kettle Brand     2400.91
# 3             Grey Poupon      743.79
# 8                  Quaker       32.42

# Tostitos has the most total spend among users created within the past six months.


# 6. Which brand has the most transactions among users who were created within the past 6 months?

# using the same joined users, receipts, and brand df from above, groupby 'name' and count the number of transactions
transactions_df = merged_barcodes_df_sel.groupby('name')['barcode_values'].size().reset_index().sort_values('barcode_values',ascending=False)

print(transactions_df)

# the resulting counts look wayyyyy too small
#                      name  barcode_values
# 9                 Swanson              11
# 10               Tostitos              11
# 5            Kettle Brand               3
# 1   Cracker Barrel Cheese               2
# 4                  Jell-O               2
# 0                 Cheetos               1
# 2         Diet Chris Cola               1
# 3             Grey Poupon               1
# 6         Pepperidge Farm               1
# 7                   Prego               1
# 8                  Quaker               1
# 11                     V8               1

# so let's do the same groupby as above except this time groupby 'description_values' from the receipts df
alt_transactions_df = merged_barcodes_df_sel.groupby('description_values')['barcode_values'].size().reset_index().sort_values('barcode_values',ascending=False)

print(alt_transactions_df.head(10))

# RESULTS
#                                     description_values  barcode_values
# 430                                     ITEM NOT FOUND             111
# 925  thindust summer face mask - sun protection nec...              44
# 922  mueller austria hypergrind precision electric ...              44
# 351  HUGGIES SIMPLY CLEAN PREMOISTENED WIPE FRAGRAN...              26
# 491  KLEENEX POP UP RECTANGLE BOX FACIAL TISSUE 2 P...              15
# 479                            KLARBRUNN 12PK 12 FL OZ              15
# 1                                              1% Milk              15
# 370                               HYV GRADE A X LRG EG              13
# 207                                 DOLE ITALIAN BLEND              13
# 276                                FRSH BNLS PORK LOIN              13

# honestly, these counts seem weird too.  So I would certainly examine the data further.



