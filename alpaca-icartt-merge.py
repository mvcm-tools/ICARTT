

import datetime
import icartt
import pandas as pd
import glob
import sys
import logging
import warnings
import re

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
fp_out = str(sys.argv[2])
fp_out_hr = str(sys.argv[3])

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
        print('did not specify to removeort data with start of measurement time or end of measurement time')
        print('specify this as second argument in icartt_convert()')
        exit(1)


# get file date of creation from ICARTT dataset object
    file_doc = datetime.datetime(ict.dateOfCollection[0],ict.dateOfCollection[1],ict.dateOfCollection[2])
    ict_index = [file_doc + datetime.timedelta(seconds = x) for x in ict.data[time]]

# make pandas Dataframe to store ICARTT file output
    df = pd.DataFrame(index = ict_index, columns = var)

# drop removeeated columns (timestamp is now the index of the dataframe)
    try:
        df.drop(['Time_Start_UTC', 'Time_Stop_UTC', 'Time_Mid_UTC'], axis=1, inplace=True)
    except:
        try:
            df.drop(['Time_Start', 'Time_Stop', 'Time_Mid'], axis=1, inplace=True)
        except:
            try:
                df.drop(['start_UTC', 'stop_UTC'], axis=1, inplace=True)
            except:
                try:
                    df.drop(['time_UTC'], axis=1, inplace=True)
                except:
                    print('No time axis dropped')
                    pass
    
    # make datetime index AKST
    df.index = df.index - pd.Timedelta('9H')
    df.index.rename('datetime_AKST', inplace=True)

    var = [x for x in df.columns]

# add group and site name to column headers
    for xi in range(0, len(df.columns)):
        newname = []
        col = []
        # if multiple sites and instruments, keep original column names
        if site_name == 'Multiple' and inst == 'Multiple':
            pass
        # if multiple sites but one instrument, add instrument to column
        elif site_name == 'Multiple' and inst != float('NaN'):
            newname = inst + df.columns[xi]
            df.rename(columns={df.columns[xi]:newname}, inplace=True)
        # if multiple sites and no instrument, keep original column names
        elif site_name == 'Multiple' and inst == float('NaN'):
            pass
        # if the instrument name and site name are already in the column name, keep original column names
        elif inst in df.columns[xi] and site_name in df.columns[xi]:
            pass
        # if only the instrument name is in the column name, add site name and rearrange to be in site, instruemnt, variable order
        elif inst in df.columns[xi] and site_name not in df.columns[xi]:
            print('rearranging column name string')
            col = []
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
            col = []
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

# return pandas dataframe
    return(df)

# list of files to merge files = [file1, file2 ...]
files = glob.glob(fp_in)

# empty list for filling with current version of icartt data
df_list = []
# seperate list for AERIS data which needs to be concatenated before merging
aeris = []

for xi in range(0, len(vers)):
    filebase = vers['file_extension'][xi]
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

df_merge.drop(['datetime_string_UTC', 'datetime_string_AKST', 'CTC_AERIS_local_fractional_day_of_year_Mid', 'CTC_COFFEE_local_fractional_day_of_year_Mid', 'fractional_day_of_year_AKST'], axis=1, inplace=True)

# get hourly avg data
df_merge_hr = df_merge.resample('1H').mean()

# cleaning up merged file before writing
print('columns in merged file: ')
print(df_merge.columns)

# write merged data to text file
df_merge.to_csv(fp_out, sep = '\t', na_rep = 'NaN', float_format='%.4f')
df_merge_hr.to_csv(fp_out_hr, sep = '\t', na_rep = 'NaN', float_format='%.4f')

print('File merged, wrote data to files "'+fp_out+'" and "'+fp_out_hr)


##################################################################
############ check merged file against original files ############
##################################################################
# uncomment this code if you want to double check the merged data against the original data

#import matplotlib.pyplot as plt

#ict_vars = ['ADEC_advisory_flag_Fairbanks', 'NH3_ppb', 'NH3_ppb', 'H2CO_ppb', 'CO_ppm', 'SO4', 'BC_smth', 'CTC_temp_6m_C', 'CH2O_COFFEE', 'CH2O_AERIS']
#df_vars = ['ADEC_advisory_flag_Fairbanks', 'House_indoor_PicarroG2103_NH3_ppb', 'House_outdoor_PicarroG2103_NH3_ppb', 'House_PicarroG2307_H2CO_ppb', 'House_PicarroG2401_CO_ppm', 'House_AMS_SO4', 'House_AE33_BC_smth', 'CTC_temp_6m_C', 'CTC_COFFEE_CH2O_COFFEE', 'CTC_AERIS_CH2O_AERIS']

#print(len(ict_vars))
#print(len(df_vars))
#print(len(df_list))


#for xi in range(0, len(ict_vars)):
#        plt.plot(df_list[xi][df_vars[xi]].dropna(), color='red')
#        plt.plot(df_merge[df_vars[xi]].dropna(), color='black', linestyle='dashed')
#        plt.ylabel(df_vars[xi])
#        plt.xlabel('Datetime (UTC)')
#        plt.show()