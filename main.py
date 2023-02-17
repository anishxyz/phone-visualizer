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


def setup_frame():
    call_data['date'] = pd.to_datetime(call_data['date'], format='%m/%d/%Y %I:%M %p')
    call_data['minutes'] = call_data['minutes'].str.extract(r'(\d+)').astype(int)
    call_data['number'] = call_data['number'].str.replace('(', '').str.replace(')', '')\
        .str.replace('-', '').str.replace(' ', '').replace('', np.nan).astype(float).astype(pd.Int64Dtype())


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

    # Calculate ratio of outgoing to incoming calls
    # result['outgoing_to_incoming_ratio'] = result['outgoing_count'] / result['incoming_count']

    return result


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print(project_path)
    print(folder_path)
    get_dataframe()
    setup_frame()
    print(call_data)
    print(analysis())
    analysis().to_csv('output.csv', sep='\t')

