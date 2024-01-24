Code developed to collect information about ASes and their internet connections.

The version currently posted is not yet the final version.

The config.py file contains some configurations, like folders name, number of threads and some variables.

The donwload_files.py code is responsible for downloading the necessary information from collectors and CAIDA, its syntax is as follows:

python3 donwload_files.py -y (inital year) -m (initial month) -d (initial day) -D (how many days) -r (download files again y/n) -p (rv/ripe projects) - t (file type, rib or updates) -a (y for all files or n for one file per day/collector)
-y inital year, e.g. 2023
-m inital month, e.g. 01
-d inital day, e.g. 01
-D how many days from the initial day, e.g. 30
-r y to download again or n to ignore already downloaded files
-p with default value rv and ripe, only inform if the download is to be performed only for one project
-t must be used to inform the type of files to be downloaded (rib or updates)
-a all for all files in the period or n to download only the first file of each day from each collector

donwload_files.py depends:
- progressbar2;
- beautifulsoup4

The reading_files.py code converts all RIB files downloaded to information files to compute features from ASes and its connections:
Depends:
- previous bgpscanner installation;
- siblings file download and set on line 13 in the file (from https://github.com/InetIntel/Improving-Inference-of-Sibling-ASes)
- Other informations set in config.py

The consolidade_period.py create information files for a set period (the initial day and the period is set in config.py file)
Depends:
- rov (https://github.com/InternetHealthReport/route-origin-validator);
- numpy;
- progressbar2

The create_graph.py export ASes and its connections information to pdf files.
Depends:
- networkx (https://networkx.org/documentation/latest/auto_examples/index.html and https://pygraphviz.github.io/documentation/stable/install.html)
- matplotlib

Example:

python3 create_graph.py -a 123

or

python3 create_graph.py -a 123,124,125

  Soon the the files will be updated.

  
