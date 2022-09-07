# ICARTT
Python scripts for reading and writing ICARTT datasets

ICARTT is a NASA file format as described in detail here:
<link> https://www-air.larc.nasa.gov/missions/etc/IcarttDataFormat.htm </link>


<p>
</p>

<P>
For merging all ALPACA ICARTT files:
Usage: ./alpaca-icartt-merge.py 'input_directory/*.ict' 'output_filename.txt'
</p>
<br>
<br>
<p>
- Need to have the python ICARTT package v2.0.0 installed (https://pypi.org/project/icartt/).
<br>
- Also need to have the spreadsheet "icartt-current-version.csv" containing variables: PI	site, instrument, icartt_version, file_extension, data_directory, data_description, notes.
</p>
