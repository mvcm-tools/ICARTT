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
Usage: ./alpaca-icartt-merge.py 'input_directory/*.ict' 'output_filename_5min.txt' 'output_filename_1hr.txt'
</p>

<p>
 <br>
- Need to have the python ICARTT package v2.0.0 installed (https://pypi.org/project/icartt/).
<br>
- Also need to have the spreadsheet "icartt-current-version.csv" containing variables: PI	site, instrument, icartt_version, file_extension, data_directory, data_description, notes.
</p>
