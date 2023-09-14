import pandas as pd

# Define a function to be applied
def nifty_trend(current,max_value,min_value):

    global nifty_buffer_value
    if(current < min_value * (1 - nifty_buffer_value) ):
        return "DOWNWARD"
    elif(current > max_value * (1 + nifty_buffer_value) ):
        return "UPWARD"
    else:
        return "NEUTRAL"

def bank_trend(current,max_value,min_value):

    global bank_buffer_value
    if(current < min_value * (1 - bank_buffer_value) ):
        return "DOWNWARD"
    elif(current > max_value * (1 + bank_buffer_value) ):
        return "UPWARD"
    else:
        return "NEUTRAL"

# Example condition function
def condition_func(value_1,value_2):
    bnifty_change = (value_2 - value_1)
    #print("Bank NIFTY Change : " + str(bnifty_change))
    return bnifty_change
    
# Define a custom function to categorize scores
def categorize_diff(diff):
    if diff > 0:
        return 1
    elif diff < 0:
        return -1
    else:
        return 0
    
# 01 - Define Parameters    
#### Variables #####
no_of_company = 6 # Including NIFTY
working_date = '2023-08-07'
time_shift=60 # In seconds
bank_buffer_value = 0.02/100 #Percentage 
nifty_buffer_value = 0.08/100 #Percentage
rolling_trend = 40   # 40*3 = 120 [ In Production code we are counting 1 for each]
rolling_rsi=50
trend_count_buffer = 25
nifty_profit_loss_change = 50
file_path = "../output/" + working_date + "_output.log"
#### Variables #####

# 02 - Read the data in Raw Dataframe    
#raw_df = pd.read_csv(f"D:\\06.STOCK Project\\01_220_STRATEGY\\backtest\\data\\raw_data_{working_date}.csv")
raw_df = pd.read_csv(f"..\\data\\raw_data_{working_date}.csv")
#print(raw_df)

# 03 - Select columns to filter    
columns = ['timestamp','d_date','t_time','c_count','company_code','current']

raw_df = raw_df[columns]

# Overwrite file with blank then write in w+ mode
with open(file_path, 'w') as file:
    file.write("")
    file.close()

with open(file_path, 'a+') as file:
    file.write(f"*********** Parameters ***********\n")
    file.write(f"no_of_company={no_of_company}\n")
    file.write(f"working_date={working_date}\n")
    file.write(f"time_shift={time_shift}\n")
    file.write(f"bank_buffer_value={bank_buffer_value}\n")
    file.write(f"nifty_buffer_value={nifty_buffer_value}\n")
    file.write(f"rolling_trend={rolling_trend}\n")
    file.write(f"trend_count_buffer={trend_count_buffer}\n")
    file.write(f"nifty_profit_loss_change={nifty_profit_loss_change}\n")
    file.write(f"*********** Parameters ***********\n")
    file.close()


# 04 - Filter Data on specific date for analysis    
df = raw_df[raw_df["d_date"] == working_date ]
print(df)

# 05 - Compute dataframe for individual company    
nifty_data = df[df["company_code"]=='NIFTY BANK']
hdfc_data = df[df["company_code"]=='HDFCBANK']
icici_data = df[df["company_code"]=='ICICIBANK']
axis_data = df[df["company_code"]=='AXISBANK']
kotak_data = df[df["company_code"]=='KOTAKBANK']
sbi_data = df[df["company_code"]=='SBIN']

# 05 - Merging HDFC BANK
merged_df = pd.merge(nifty_data, hdfc_data, on='c_count', how='inner')
merged_df.rename(columns={'current_x': 'nifty_current','current_y': 'hdfc_current'}, inplace=True)
new_columns = ['timestamp_x','d_date_x','t_time_x','c_count','nifty_current','hdfc_current']
merged_df = merged_df [new_columns]

# 06 - Merging ICICI BANK
merged_df = pd.merge(merged_df, icici_data, on='c_count', how='inner')
merged_df.rename(columns={'current': 'icici_current'}, inplace=True)
new_columns = ['timestamp_x','d_date_x','t_time_x','c_count','nifty_current','hdfc_current','icici_current']
merged_df = merged_df [new_columns]

# 07 - Merging AXIS BANK
merged_df = pd.merge(merged_df, axis_data, on='c_count', how='inner')
merged_df.rename(columns={'current': 'axis_current'}, inplace=True)
new_columns = ['timestamp_x','d_date_x','t_time_x','c_count','nifty_current','hdfc_current','icici_current','axis_current']
merged_df = merged_df [new_columns]

# 07.01 - Merging KOTAK BANK
merged_df = pd.merge(merged_df, kotak_data, on='c_count', how='inner')
merged_df.rename(columns={'current': 'kotak_current'}, inplace=True)
new_columns = ['timestamp_x','d_date_x','t_time_x','c_count','nifty_current','hdfc_current','icici_current','axis_current','kotak_current']
merged_df = merged_df [new_columns]

# 07.01 - Merging SBI BANK
merged_df = pd.merge(merged_df, sbi_data, on='c_count', how='inner')
merged_df.rename(columns={'current': 'sbi_current'}, inplace=True)
new_columns = ['timestamp_x','d_date_x','t_time_x','c_count','nifty_current','hdfc_current','icici_current','axis_current','kotak_current','sbi_current']
merged_df = merged_df [new_columns]



# 08 - Sorting Final Dataframe in Ascending Order
merged_df = merged_df.sort_values(by='c_count', ascending=True)
min_max_df = merged_df.copy() 

# 09 - Stripping time take only upto minutes
merged_df['t_time_strip'] = merged_df['t_time_x'].astype(str).str[:5]

# 10 - Computing min - max of the companies for trend, by adding time shift in seconds
min_max_df['t_time_strip'] = min_max_df['t_time_x'] + pd.Timedelta(seconds=time_shift)
min_max_df['t_time_strip'] = min_max_df['t_time_strip'].astype(str).str[:12].str[7:]

# 11 - Grouping data and assigning min-max values for each company
result = min_max_df.groupby('t_time_strip').agg(min_nifty_current=pd.NamedAgg(column='nifty_current', aggfunc='min'),
                                      max_nifty_current=pd.NamedAgg(column='nifty_current', aggfunc='max'),
                                      min_hdfc_current=pd.NamedAgg(column='hdfc_current', aggfunc='min'),
                                      max_hdfc_current=pd.NamedAgg(column='hdfc_current', aggfunc='max'),
                                      min_icici_current=pd.NamedAgg(column='icici_current', aggfunc='min'),
                                      max_icici_current=pd.NamedAgg(column='icici_current', aggfunc='max'),
                                      min_axis_current=pd.NamedAgg(column='axis_current', aggfunc='min'),
                                      max_axis_current=pd.NamedAgg(column='axis_current', aggfunc='max'),
                                      min_kotak_current=pd.NamedAgg(column='kotak_current', aggfunc='min'),
                                      max_kotak_current=pd.NamedAgg(column='kotak_current', aggfunc='max'),
                                      min_sbi_current=pd.NamedAgg(column='sbi_current', aggfunc='min'),
                                      max_sbi_current=pd.NamedAgg(column='sbi_current', aggfunc='max')
                                      )

# 11 - Joining Min Max Columns with previous candle min-max
final_df = pd.merge(merged_df, result, on='t_time_strip', how='inner')
final_df = final_df.drop(columns=['t_time_strip'])


# 12 - Adding Trend as UPWARD, DOWNWARD, NEUTRAL
final_df['nifty_trend'] = final_df.apply(lambda row: nifty_trend(row['nifty_current'], row['max_nifty_current'], row['min_nifty_current']), axis=1)
final_df['hdfc_trend'] = final_df.apply(lambda row: bank_trend(row['hdfc_current'], row['max_hdfc_current'], row['min_hdfc_current']), axis=1)
final_df['icici_trend'] = final_df.apply(lambda row: bank_trend(row['icici_current'], row['max_icici_current'], row['min_icici_current']), axis=1)
final_df['axis_trend'] = final_df.apply(lambda row: bank_trend(row['axis_current'], row['max_axis_current'], row['min_axis_current']), axis=1)
final_df['kotak_trend'] = final_df.apply(lambda row: bank_trend(row['kotak_current'], row['max_kotak_current'], row['min_kotak_current']), axis=1)
final_df['sbi_trend'] = final_df.apply(lambda row: bank_trend(row['sbi_current'], row['max_sbi_current'], row['min_sbi_current']), axis=1)

# 13 - Counting the Occurenece of Neutral only
# Values to count occurrences for
values_to_count = ["NEUTRAL"]

# Define a function to count occurrences in each row
def count_occurrences(row):
    count = sum(1 for value in row if value in values_to_count)
    return count

# Apply the function to the rows of df1 and create a new column 'count_column'
final_df['neutral_count'] = final_df.apply(count_occurrences, axis=1)

# 14 - Now count the rolling Sum of NEUTRAL
# Calculate the rolling neutral count sum using .rolling() and .sum()
final_df['rolling_neutral_count'] = final_df['neutral_count'].rolling(rolling_trend).sum()

# Additional Code - Enhancement - revision 1
# Computing RSI at the moment based on previous n values
final_df['nifty_current_diff'] = final_df['nifty_current'].diff()
final_df['up_down'] = final_df['nifty_current_diff'].apply(categorize_diff)

# Calculate the rolling sum and store it in a new column
final_df['rolling_sum'] = final_df['up_down'].rolling(window=rolling_rsi).sum()
final_df['rsi'] = final_df['rolling_sum']/rolling_rsi*100

# 15 - Define PE and CE Order for the point in time values
# Check for PE and CE orders
# Define the conditions and populate 'new_column' with custom values
threshold_for_trend = no_of_company*rolling_trend - trend_count_buffer
#final_df.loc[(final_df['rolling_neutral_count'] > threshold_for_trend ) & (final_df['nifty_trend'] == "UPWARD") & (final_df['hdfc_trend'] == "UPWARD") & (final_df['icici_trend'] == "UPWARD") & (final_df['axis_trend'] == "UPWARD") & (final_df['kotak_trend'] == "UPWARD") & (final_df['sbi_trend'] == "UPWARD") , 'order'] = 'BUY_CE'
#final_df.loc[(final_df['rolling_neutral_count'] > threshold_for_trend ) & (final_df['nifty_trend'] == "DOWNWARD") & (final_df['hdfc_trend'] == "DOWNWARD") & (final_df['icici_trend'] == "DOWNWARD") & (final_df['axis_trend'] == "DOWNWARD") & (final_df['kotak_trend'] == "DOWNWARD") & (final_df['sbi_trend'] == "DOWNWARD") , 'order'] = 'BUY_PE'

# Code Update - Neutral count is not needed, instead will be replaced by RSI later
# With RSI
final_df.loc[ (final_df['rsi'] > 15 ) & (final_df['rsi'] < 25 ) & (final_df['nifty_trend'] == "UPWARD") & (final_df['hdfc_trend'] == "UPWARD") & (final_df['icici_trend'] == "UPWARD") & (final_df['axis_trend'] == "UPWARD") & (final_df['kotak_trend'] == "UPWARD") & (final_df['sbi_trend'] == "UPWARD") , 'order'] = 'BUY_CE'
final_df.loc[ (final_df['rsi'] < -15 ) & (final_df['rsi'] > -25 ) & (final_df['nifty_trend'] == "DOWNWARD") & (final_df['hdfc_trend'] == "DOWNWARD") & (final_df['icici_trend'] == "DOWNWARD") & (final_df['axis_trend'] == "DOWNWARD") & (final_df['kotak_trend'] == "DOWNWARD") & (final_df['sbi_trend'] == "DOWNWARD") , 'order'] = 'BUY_PE'
# Without RSI
#final_df.loc[ (final_df['nifty_trend'] == "UPWARD") & (final_df['hdfc_trend'] == "UPWARD") & (final_df['icici_trend'] == "UPWARD") & (final_df['axis_trend'] == "UPWARD") & (final_df['kotak_trend'] == "UPWARD") & (final_df['sbi_trend'] == "UPWARD") , 'order'] = 'BUY_CE'
#final_df.loc[ (final_df['nifty_trend'] == "DOWNWARD") & (final_df['hdfc_trend'] == "DOWNWARD") & (final_df['icici_trend'] == "DOWNWARD") & (final_df['axis_trend'] == "DOWNWARD") & (final_df['kotak_trend'] == "DOWNWARD") & (final_df['sbi_trend'] == "DOWNWARD") , 'order'] = 'BUY_PE'

# 15 - Generate final order table
# Check the 'column_name' for non-NaN values with boolean series
non_nan_mask = final_df['order'].notna()
order_df = final_df[non_nan_mask]

# 16 - Collect Index and process based on it
index_list = order_df.index.tolist()
#start_index = 152

profit_count=0
loss_count=0
for start_index in index_list:
    
    order_type = final_df.loc[start_index,'order']    
    # Applying the function row by row
    for index, row in final_df.iterrows():
        if index < start_index:
            continue  # Skip rows until start_index is reached
    
        value_1 = final_df.loc[start_index,'nifty_current']
        value_2 = final_df.loc[index,'nifty_current']
        value = condition_func(value_1, value_2)
        final_df.loc[index,"trend_" + str(start_index)] = value
    
        if value > nifty_profit_loss_change and order_type == "BUY_CE" :
            # Perform your desired action here
            condition_time = final_df.loc[start_index,'timestamp_x']
            end_time = final_df.loc[index,'timestamp_x']
            rsi = final_df.loc[start_index,'rsi']
            output_log = f"Profit Occured for RSI = {rsi} order {order_type} for NIFTY {value_1} at time {condition_time}, at {end_time}\n"
            profit_count=profit_count+1
            print(output_log)
            with open(file_path, 'a+') as file:
                file.write(output_log)
                file.close()
            break  # Exit the loop once the condition is met
        elif value < -nifty_profit_loss_change and order_type == "BUY_PE" :
            # Perform your desired action here
            condition_time = final_df.loc[start_index,'timestamp_x']
            end_time = final_df.loc[index,'timestamp_x']
            rsi = final_df.loc[start_index,'rsi']
            output_log=f"Profit Occured for RSI = {rsi} order {order_type} for NIFTY {value_1} at time {condition_time}, at {end_time}\n"
            profit_count=profit_count+1
            print(output_log)
            with open(file_path, 'a+') as file:
                file.write(output_log)
                file.close()
            break  # Exit the loop once the condition is met
        elif value > nifty_profit_loss_change and order_type == "BUY_PE" :
            # Perform your desired action here
            condition_time = final_df.loc[start_index,'timestamp_x']
            end_time = final_df.loc[index,'timestamp_x']
            rsi = final_df.loc[start_index,'rsi']
            output_log=f"Loss Occured for RSI = {rsi} order {order_type} for NIFTY {value_1} at time {condition_time}, at {end_time}\n"
            loss_count=loss_count+1
            print(output_log)
            with open(file_path, 'a+') as file:
                file.write(output_log)
                file.close()
            break  # Exit the loop once the condition is met
        elif value < -nifty_profit_loss_change and order_type == "BUY_CE" :
            # Perform your desired action here
            condition_time = final_df.loc[start_index,'timestamp_x']
            end_time = final_df.loc[index,'timestamp_x']
            rsi = final_df.loc[start_index,'rsi']
            output_log=f"Loss Occured for RSI = {rsi} order {order_type} for NIFTY {value_1} at time {condition_time}, at {end_time}\n"
            loss_count=loss_count+1
            print(output_log)
            with open(file_path, 'a+') as file:
                file.write(output_log)
                file.close()
            break  # Exit the loop once the condition is met

profit_percentage = (profit_count)/(profit_count + loss_count)*100
loss_percentage = (loss_count)/(profit_count + loss_count)*100
profit_message = "Profit Percentage : " + str(round(profit_percentage,2))
loss_message = "Loss Percentage : " + str(round(loss_percentage,2))
print(profit_message)   
print(loss_message)   
with open(file_path, 'a+') as file:
    file.write(profit_message + "\n")
    file.write(loss_message + "\n")
    file.close()     