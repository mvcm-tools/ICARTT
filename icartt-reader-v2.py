#!/usr/bin/env python3

import datetime
import icartt
import pandas as pd
from functools import reduce
import glob
import sys

print('Usage: '+str(sys.argv[0])+' icartt_directory/*.ict output_filename.txt')
print('')
print('')

fp_in = str(sys.argv[1])
fp_out = str(sys.argv[2])

#########################################
# read the icartt data
#########################################
# function to convert icartt dataset to pandas dataframe
def icartt_convert(input_file, start_stop):
    
    print('################')
    print('currently processing file:')
    print(input_file)
    print('################')
    print('')

    ict = icartt.Dataset(input_file)
# get column names from the ICARTT dataset object
    var = ict.variables.keys()

# choose timestamp column name
    if start_stop == 'start':
        if 'Time_Start_UTC' in var:
            time = 'Time_Start_UTC'
        elif 'Time_Start' in var:
            time = 'Time_Start'
        elif 'start_UTC' in var:
            time = 'start_UTC'
        elif 'time_UTC' in var:
            time = 'time_UTC'
            print('Start and stop time not specified, found "time_UTC" in the column header')
        else:
            print('Time columns are not named "Time_Start", "start_UTC" or "Time_Start_UTC", stopping program.')
            print('')
            exit(1)
    elif start_stop == 'stop':
        if 'Time_Stop_UTC' in var:
            time = 'Time_Stop_UTC'
        elif 'Time_Stop' in var:
            time = 'Time_Stop'
        elif 'stop_UTC' in var:
            time = 'stop_UTC'
        elif 'time_UTC' in var:
            time = 'time_UTC'
            print('Start and stop time not specified, found "time_UTC" in the column header')
        else:
            print('Time columns are not named "Time_Stop", "stop_UTC" or "Time_Stop_UTC", stopping program.')
            print('')
            exit(1)
    else:
        print('did not specify to report data with start of measurement time or end of measurement time')
        print('specify this as second argument in icartt_convert()')
        exit(1)


# get file date of creation from ICARTT dataset object
    file_doc = datetime.datetime(ict.dateOfCollection[0],ict.dateOfCollection[1],ict.dateOfCollection[2])
    ict_index = [file_doc + datetime.timedelta(seconds = x) for x in ict.data[time]]
# make pandas Dataframe to store ICARTT file output
    df = pd.DataFrame(index = ict_index, columns = var)

# add data from ICARTT dataset object to pandas dataframe
    for variable in var:
        df[variable] = ict.data[variable]

# if data is from Meeta and Bill, make sure datetime string columns are not numeric
    if input_file == 'gasandmet_ctcbuilding_20220101_R0.ict':
    # drop extra UTC datetime string (already in index)
        df.drop(['datetime_string_UTC'], axis=1, inplace=True)
    # convert AKST from numeric to string
        df['datetime_string_AKST'] = pd.to_datetime(df['datetime_string_AKST'], format='%Y%m%d%H%M%S').dt.strftime('%Y-%m-%d %H:%M:%S')
    else:
        pass

# drop repeated columns (timestamp is now the index of the dataframe)
    try:
        df.drop(['Time_Start_UTC', 'Time_Mid_UTC', 'Time_Stop_UTC'], axis=1, inplace=True)
    except:
        try:
            df.drop(['Time_Start', 'Time_Mid', 'Time_Stop'], axis=1, inplace=True)
        except:
            try:
                df.drop(['start_UTC', 'stop_UTC'], axis=1, inplace=True)
            except:
                try:
                    df.drop(['time_UTC'], axis=1, inplace=True)
                except:
                    pass
    
    df.index.rename('datetime_UTC', inplace=True)
# return pandas dataframe
    return(df)

# list of files to merge files = [file1, file2 ...]
files = glob.glob(fp_in)

print('merging files: ')
print(files)
print('')
print('')

# empty list for filling with data
df_list = []

# add data from each file in list
# if using measurement stop time as the reporting timestamp, just change strings below as needed
for file in files:
    add = icartt_convert(file, 'start')
    df_list.append(add)

# merge all dataframes in the list together by index
df_merge = reduce(lambda x, y: pd.merge(x, y, how="outer", left_index=True, right_index=True), df_list)

# write merged data to text file
df_merge.to_csv(fp_out, sep = '\t', na_rep = 'NaN', float_format='%.4f')

print('File merged, wrote data to file "'+fp_out+'"')