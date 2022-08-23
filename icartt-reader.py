import datetime
import icartt
import pandas as pd
from functools import reduce

#########################################
# read the icartt data
#########################################
# function to convert icartt dataset to pandas dataframe
def icartt_convert(input_file, start_stop):
    if start_stop == 'start':
        time = 'Time_Start_UTC'
    elif start_stop == 'stop':
        time = 'Time_Stop_UTC'
    else:
        print('did not specify to report data with start of measurement time or end of measurement time')
        print('specify this as second argument in icartt_convert()')
        exit(1)

    ict = icartt.Dataset(input_file)
# get column names from the ICARTT dataset object
    var = ict.variables.keys()
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
    df.drop(['Time_Start_UTC', 'Time_Mid_UTC', 'Time_Stop_UTC'], axis=1, inplace=True)
    df.index.rename('datetime_UTC', inplace=True)
# return pandas dataframe
    return(df)

# list of files to merge files = [file1, file2 ...]
files = ['gasandmet_ctcbuilding_20220101_R0.ict']

# empty list for filling with data
df_list = []

# add data from each file in list
# if using measurement stop time as the reporting timestamp, just change strings below as needed
for file in files:
    add = icartt_convert(file, 'start')
    df_list.append(add)

# merge all dataframes in the list together by index
df_merge = reduce(lambda x, y: pd.merge(x, y, how="outer", left_index=True, right_index=True), df_list)

print('File merged:')
print('')
print(df_merge)
print('')
print('')

# write merged data to text file
df_merge.to_csv('outfile-example.txt', sep = '\t', na_rep = 'NaN', float_format='%.4f')