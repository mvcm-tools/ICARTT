# ICARTT
Python script for merging ALPACA ICARTT datasets.

ICARTT is a NASA file format as described in detail here:
<link> https://www-air.larc.nasa.gov/missions/etc/IcarttDataFormat.htm </link>


<p>
</p>

<P>
<br>
For merging all ALPACA ICARTT files: 
<br>
<br>
V1-2
Usage: ./alpaca-icartt-merge-v1-2.py 'input_directory/*.ict' 'output_filebase'
<br>
'output_filebase' should be the file base name, the time resolution of the file will be added to the file base name as '-timeres.txt'
<br>
<br>
V1-1
Usage: ./alpaca-icartt-merge-v1-1.py 'input_directory/*.ict' 'output_filename_5min.txt' 'output_filename_hr.txt'
</p>

<p>
 <br>
- Need to have the python ICARTT package v2.0.0 installed (https://pypi.org/project/icartt/).
<br>
- Also need to have the spreadsheet "icartt-current-version.csv" containing variables: PI	site, instrument, icartt_version, file_extension, data_directory, data_description, notes.
</p>
