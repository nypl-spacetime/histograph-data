# Histograph Data

Histograph's data tool loads separate data modules which perform ETL tasks.

## Configuration

The configuration of the data tool is done in the [Histograph configuration file](https://github.com/histograph/config), under the `data` key:

| Parameter      | Description
|----------------|-----------------------------------------
| `baseDir`      | Path (absolute, or relative to data tool) where data tool looks for data modules
| `modulePrefix` | Directory prefix used to identify data modules (e.g. `data-tgn`)
| `outputDir` | Directory to which data modules write their data

The configuration of the separate data modules can also be done in configuration file.

## Installation

First, clone this repository and the repository of the data modules you need:

    git clone https://github.com/histograph/data.git

    # Data module repositories:
    git clone https://github.com/histograph/data-tgn.git
    git clone https://github.com/histograph/data-geonames.git
    git clone https://github.com/histograph/data-bag.git
    git clone https://github.com/histograph/data-nwb.git

Then, install dependencies:

    cd data
    npm install
    cd ..

    # Data module repositories:    
    cd data-tgn
    npm install
    cd ..
    cd data-geonames
    npm install
    cd ..
    cd data-bag
    npm install
    cd ..
    cd data-nwb
    npm install
    cd ..

## Download and convert data

Run the data tool without command line arguments to get a list of the available data modules:

  node index.js

To execute a module, provide their dataset IDs as command line parameters:

    node index.js geonames tgn ...

Alternatively, you can select the processing steps you want to run:

    node index.js --steps=convert tgn geonames

By default, all steps are run consecutively.

Copyright (C) 2015 [Waag Society](http://waag.org).
