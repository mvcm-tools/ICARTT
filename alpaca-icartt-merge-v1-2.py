

import datetime
import icartt
import pandas as pd
import glob
import sys
import logging
import warnings
import re
import numpy as np

# open file to log warnings
logging.basicConfig(filename="icartt-warnings.txt",level=logging.DEBUG)
logging.captureWarnings(True)

warning_file = open("icartt-warnings.txt", "w")

# function to log warnings
def mywarning(message, category, filename, lineno, line=None):
    warning_file.write(warnings.formatwarning(message, category, filename, lineno, line))


# show usage of script
print('Usage: '+str(sys.argv[0])+' icartt_directory/*.ict output_filename_5min.txt output_filename_hr.txt')
print('')
print('')

fp_in = str(sys.argv[1])
fp_out_base = str(sys.argv[2])

# read in version control excel sheet
vers = pd.read_csv("icartt-current-version.csv", delimiter=',')

#########################################
# read the icartt data
#########################################
# function to convert icartt dataset to pandas dataframe
def icartt_convert(input_file, site_name, inst, start_stop):
    
    print('################')
    print('currently processing file:')
    print(input_file)
    print('site_name = '+site_name)
    print('instrument = '+inst)
    print('################')
    print('')

    # open ICARTT dataset and write warnings to file
    logging.info('Warnings for file "'+input_file+'":\n\n')
    ict = icartt.Dataset(input_file)

# get column names from the ICARTT dataset object
    var = [x for x in ict.variables.keys()]
    units = [ict.variables[x].units for x in var]

# get file date of creation from ICARTT dataset object
    year = str(ict.dateOfCollection[0])
    month = str(ict.dateOfCollection[1])
    day = str(ict.dateOfCollection[2])

# add zero to month and day if needed
    if len(month) == 1:
        month = '0'+month
    else:
        pass

    if len(day) == 1:
        day = '0'+day
    else:
        pass

# make pandas Dataframe to store ICARTT file output
    df = pd.DataFrame(index = ict.times, columns = var)
    
    # make datetime index AKST
    df.index = df.index - pd.Timedelta('9H')
    df.index.rename('datetime_AKST', inplace=True)

# add group and site name to column headers
    for xi in range(0, len(df.columns)):
        newname = []
        col = []
        unit = units[xi]

        # if multiple sites and instruments, don't change columns names
        if site_name == 'Multiple' and inst == 'Multiple':
            pass
        # if multiple sites but one instrument, add instrument to column
        elif site_name == 'Multiple' and inst != float('NaN'):
            newname = inst + df.columns[xi]
            df.rename(columns={df.columns[xi]:newname}, inplace=True)
        # if multiple sites and no instrument, don't change columns names
        elif site_name == 'Multiple' and inst == float('NaN'):
            pass
        # if the instrument name and site name are already in the column name, don't change columns names
        elif inst in df.columns[xi] and site_name in df.columns[xi]:
            pass
        # if only the instrument name is in the column name, add site name and rearrange to be in site, instruemnt, variable order
        elif inst in df.columns[xi] and site_name not in df.columns[xi]:
            print('rearranging column name string')
            if '_'+inst+'_' in df.columns[xi]:
                col.append(re.sub('_'+inst+'_', '', df.columns[xi]))
            elif '_'+inst in df.columns[xi]:
                col.append(re.sub('_'+inst, '', df.columns[xi]))
            else:
                col.append(re.sub(inst+'_', '', df.columns[xi]))
            newname = site_name+'_'+inst+'_'+col[0]
            df.rename(columns={df.columns[xi]:newname}, inplace=True)
        # if only the site name is in the column name, add instrument name and rearrange to be in site, instruemnt, variable order
        elif site_name in df.columns[xi] and inst not in df.columns[xi]:
            print('rearranging column name string')
            if '_'+site_name+'_' in df.columns[xi]:
                col.append(re.sub('_'+site_name+'_', '', df.columns[xi]))
            elif '_'+site_name in df.columns[xi]:
                col.append(re.sub('_'+site_name, '', df.columns[xi]))
            else:
                col.append(re.sub(site_name+'_', '', df.columns[xi]))
            newname = site_name+'_'+inst+'_'+col[0]
            df.rename(columns={df.columns[xi]:newname}, inplace=True)
        # if column name does not have an instrument or site name already, add both instrument and sitename to column name in site, instruemnt, variable order
        else:
            newname = site_name+'_'+inst+'_'+df.columns[xi]
            df.rename(columns={df.columns[xi]:newname}, inplace=True)

        if 'nan' in df.columns[xi]:
            newname = re.sub('nan', '', df.columns[xi])
            df.rename(columns={df.columns[xi]:newname}, inplace=True)
        else:
            pass

    # units are taken from the icartt variables "var" and the original df columns are created from the object "var", so they can all use the same iterator xi
    # if unit contains spaces but isn't a microgram per meter cubed variable, don't add it
        if ' ' in unit and 'ug m-3' not in unit:
            pass
    # if the unit is microgram per meter cubed , add it in common format
        elif 'ug m-3' in unit:
            newname = str(df.columns[xi])+'_ug_m3'
            df.rename(columns={df.columns[xi]:newname}, inplace=True)
    # if the unit is already in the column name, don't add it
        elif unit in df.columns[xi]:
            pass
    # don't add units if the unit is "flag_txt" from the Decarlo group's data
        elif unit == 'flag_txt':
            pass
    # add unit with to end of column name
        else:
            newname = str(df.columns[xi])+'_'+str(unit)
            df.rename(columns={df.columns[xi]:newname}, inplace=True)


# make sure the length of the original icartt variables matches the length of the new dataframe's columns, if not quit the script
    if len(df.columns) != len(var):
        print('# of columns in dataframe and icartt variables do not match, bailing')
        exit(1)
    else:
        print('df cols:')
        print(df.columns)
        print('icartt vars:')
        print(var)
        pass


# add data from ICARTT dataset object to pandas dataframe
    for xi in range(0, len(df.columns)):
        df[df.columns[xi]] = ict.data[var[xi]]


# drop any columns that have "time/Time/fractional_day" in the column name
    cols_before = df.columns
    
    try:
        df = df[df.columns.drop(list(df.filter(regex='time')))]
        cols_new = df.columns
        print('removed columns: '+str(cols_before.drop(cols_new)))
    except:
        pass

    try:
        df = df[df.columns.drop(list(df.filter(regex='Time')))]
        cols_new = df.columns
        print('removed columns: '+str(cols_before.drop(cols_new)))
    except:
        pass

    try:
        df = df[df.columns.drop(list(df.filter(regex='fractional_day')))]
        cols_new = df.columns
        print('removed columns: '+str(cols_before.drop(cols_new)))
    except:
        pass


    try:
        df = df[df.columns.drop(list(df.filter(regex='UTC')))]
        cols_new = df.columns
        print('removed columns: '+str(cols_before.drop(cols_new)))
    except:
        pass


# return pandas dataframe
    return(df)

# list of files to merge files = [file1, file2 ...]
files = glob.glob(fp_in)

# empty list for filling with current version of icartt data
df_list = []
# seperate list for AERIS data which needs to be concatenated before merging
aeris = []

for xi in range(0, len(vers)):
    filebase = vers['file_base'][xi]
    vers_num = vers['icartt_version'][xi]
    site = str(vers['site'][xi])
    instrument = str(vers['instrument'][xi])
    print('filebase:')
    print(filebase)
    print('version_num')
    print(vers_num)
    print('')
    print('')
    # add AERIS data to AERIS data list
    if '.ict' in filebase and 'AERIS' in filebase:
        print('adding data from '+filebase+' to AERIS list')
        print('...')
        print('')
        print('')
        add = icartt_convert(filebase, site, instrument, 'start')
        print('Timestamp time difference: ')
        dT = pd.Timedelta(add.index[1] - add.index[0])
        print(dT)
        add = add.resample('5T').mean()
        print('')
        print('')
        aeris.append(add)
    # add other ICARTT files that don't use the "RX" version # extension to other list
    elif '.ict' in filebase and 'AERIS' not in filebase:
        print('adding data from '+filebase+' to main list')
        print('...')
        print('')
        print('')
        add = icartt_convert(filebase, site, instrument, 'start')
        print('Timestamp time difference: ')
        dT = pd.Timedelta(add.index[1] - add.index[0])
        print(dT)
        add = add.resample('5T').mean()
        print('')
        print('')
        df_list.append(add)
    # add other ICARTT files that use the "RX" version # extension to other list
    else:
        for file in files: 
            if filebase in file and 'R'+str(vers_num) in file:  
                print('adding data from '+file+' to main list')
                print('...')
                print('')
                print('')
                add = icartt_convert(file,  site, instrument, 'start')
                print('Timestamp time difference: ')
                dT = pd.Timedelta(add.index[1] - add.index[0])
                print(dT)
                add = add.resample('5T').mean()
                print('')
                print('')
                df_list.append(add)
            else:
                pass

# concatenate AERIS data
aeris_merge = pd.concat(aeris)

# add aeris data to ICARTT data list
df_list.append(aeris_merge)

df_merge = df_list[0].copy(deep=False)

for xi in range(1, len(df_list)):
    df_merge = df_merge.merge(df_list[xi], how="outer", left_index=True, right_index=True)


# cut to 2022-01-01 AKST
df_merge = df_merge['2022-01-01':'2022-02-28']

# get hourly avg data
df_merge_hr = df_merge.resample('1H').mean()


start = [' 07:00:00', ' 09:30:00', ' 09:30:00', ' 17:30:00', ' 10:00:00', ' 09:00:00', ' 09:00:00', ' 21:00:00']
end = [' 07:00:00', ' 09:00:00', ' 17:00:00', ' 09:00:00', ' 09:30:00', ' 09:00:00', ' 21:00:00', ' 09:00:00']
hrs = ['24H', '23.5H', '7.5H', '11.5', '23.5', '24H', '12H', '12H']
same_day = ['no', 'no', 'yes', 'no', 'no', 'no', 'yes', 'no']

days = pd.date_range('2022-01-01', '2022-02-28', freq='1D')

ls = []

for xi in range(0, len(start)):
    days_index = pd.date_range('2022-01-01'+start[xi], '2022-02-28'+start[xi], freq='1D')
    df = pd.DataFrame(index=days_index, columns=df_merge.columns.drop(['CTC_COFFEE_CH2O_ppbv', 'CTC_AERIS_CH2O_ppbv']))
    for xn in range(0,len(days)):
        if same_day[xi] == 'yes':
            ser = round(df_merge[str(days[xn])+start[xi]:str(days[xn])+end[xi]].mean(axis=0), 4)
            df.loc[days_index[xn], :] = ser
        else:
            ser = round(df_merge[str(days[xn])+start[xi]:str(days[xn]+pd.Timedelta('1D'))+end[xi]].mean(axis=0), 4)
            df.loc[days_index[xn], :] = ser
    ls.append(df)
    


# cleaning up merged file before writing
print('columns in merged file: ')
print(df_merge.columns)

# write merged data to text file
df_merge.to_csv(fp_out_base+'-5min.txt', sep = '\t', na_rep = 'NaN', float_format='%.4f')
df_merge_hr.to_csv(fp_out_base+'-hr.txt', sep = '\t', na_rep = 'NaN', float_format='%.4f')

# write data for filter samples
ls[0].to_csv(fp_out_base+'-7am-7am.txt', sep = '\t', na_rep = 'NaN', float_format='%.4f')
ls[1].to_csv(fp_out_base+'-930am-9am.txt', sep = '\t', na_rep = 'NaN', float_format='%.4f')
ls[2].to_csv(fp_out_base+'-930am-5pm.txt', sep = '\t', na_rep = 'NaN', float_format='%.4f')
ls[3].to_csv(fp_out_base+'-530pm-9am.txt', sep = '\t', na_rep = 'NaN', float_format='%.4f')
ls[4].to_csv(fp_out_base+'-10am-930am.txt', sep = '\t', na_rep = 'NaN', float_format='%.4f')
ls[5].to_csv(fp_out_base+'-9am-9am.txt', sep = '\t', na_rep = 'NaN', float_format='%.4f')
ls[6].to_csv(fp_out_base+'-9am-9pm.txt', sep = '\t', na_rep = 'NaN', float_format='%.4f')
ls[7].to_csv(fp_out_base+'-9pm-9am.txt', sep = '\t', na_rep = 'NaN', float_format='%.4f')


print('File merged, wrote data to files "'+fp_out_base+'-5min.txt" and "'+fp_out_base+'-hr.txt"')


