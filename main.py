import csv
import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

pd.set_option('display.max_columns', None)

# path to project directory
project_path = os.path.abspath(os.getcwd())

# path to data folder (store .csv logs)
folder_path = os.path.join(project_path, 'data')

# dataframe with all call logs
call_data = []


# print data of all .csv logs
def print_files():
    for filename in os.listdir(folder_path):
        if filename.endswith('.csv'):
            file_path = os.path.join(folder_path, filename)
            with open(file_path, newline='') as csvfile:
                reader = csv.reader(csvfile, delimiter=',')
                for i, row in enumerate(reader):
                    if i >= 8:
                        print(row)


# builds dataframe from csv files stored in data
def get_dataframe():
    global call_data
    temp = []
    for filename in os.listdir(folder_path):
        if filename.endswith('.csv'):
            file_path = os.path.join(folder_path, filename)
            with open(file_path, newline='') as csvfile:
                reader = csv.reader(csvfile)
                # skip the first 7 rows
                for i in range(7):
                    next(reader)
                # read until the first empty row
                data = []
                for row in reader:
                    if not any(row):
                        break
                    data.append(row)
                df = pd.DataFrame(data=data, columns=["date", "number", "location", "duration", "type"])
                temp.append(df)
                # print(df)
    call_data = pd.concat(temp)
    call_data.columns = ["date", "number", "destination", "minutes", "type"]


# sets up data frame for further analysis
# cleans data
def setup_frame():
    call_data['date'] = pd.to_datetime(call_data['date'], format='%m/%d/%Y %I:%M %p')
    call_data['minutes'] = call_data['minutes'].str.extract(r'(\d+)').astype(int)
    call_data['number'] = call_data['number'].str.replace('(', '').str.replace(')', '')\
        .str.replace('-', '').str.replace(' ', '').replace('', np.nan).astype(float).astype(pd.Int64Dtype())


# analyze all the data!
# get incoming and outgoing counts
# get total time per week chatted
# get ratio on who is calling more?
# average distinct days talked per week
def analysis(mom_number=None):
    global call_data

    if mom_number is not None:
        temp = call_data[call_data['number'] == mom_number]
        call_data = temp

    # Group data by week and number
    grouped = call_data.groupby([pd.Grouper(key='date', freq='W-MON'), 'number'], group_keys=True)

    # Calculate total duration for each group
    duration = grouped['minutes'].sum()

    # Count number of incoming and outgoing calls for each group
    incoming_count = grouped['destination'].apply(lambda x: (x == 'Incoming').sum())
    outgoing_count = grouped['destination'].apply(lambda x: (x != 'Incoming').sum())

    # Combine counts and duration into a single DataFrame
    result = pd.concat([duration, incoming_count, outgoing_count], axis=1)

    # Rename columns
    result.columns = ['total_duration', 'incoming_count', 'outgoing_count']

    result['outgoing_to_incoming_ratio'] = result['outgoing_count'] / result['incoming_count']
    result['distinct_days'] = grouped.apply(
        lambda x: x[x['minutes'] > 1]['date'].dt.date.nunique()
    )

    # Calculate ratio of outgoing to incoming calls
    # result['outgoing_to_incoming_ratio'] = result['outgoing_count'] / result['incoming_count']

    if mom_number is not None:
        ignore_months = [5, 6, 7, 8, 12]
        mom_calls = result[result.index.get_level_values('number') == mom_number]
        mom_calls = mom_calls[~mom_calls.index.get_level_values('date').month.isin(ignore_months)]
        mom_calls = mom_calls.groupby(level='date').sum()  # group by date
        incoming_greater = len(mom_calls[mom_calls['incoming_count'] > mom_calls['outgoing_count']])
        outgoing_greater = len(mom_calls[mom_calls['incoming_count'] < mom_calls['outgoing_count']])
        print(f"{mom_number} called more than you in {incoming_greater} weeks.")
        print(f"You called {mom_number} more than they called you in {outgoing_greater} weeks.")
        print(f"Average distinct days with calls longer than 1 minute per week: {result['distinct_days'].mean()}")

    return result


# run it all here
if __name__ == '__main__':
    print(project_path)
    get_dataframe()
    setup_frame()
    # print(call_data)
    # print(analysis())
    analysis().to_csv('output.csv', sep='\t')

