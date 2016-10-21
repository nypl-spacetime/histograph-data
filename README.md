# Space/Time ETL tool

Extract/Transform/Load tool for Space/Time data tool. spacetime-etl loads separate data modules which perform ETL tasks.

## Configuration

The configuration of the data tool is done in the [Space/Time configuration file](https://github.com/nypl-spacetime/spacetime-config), under the `data` key:

| Parameter      | Description
|----------------|-----------------------------------------
| `baseDir`      | Path (absolute, or relative to data tool) where data tool looks for data modules
| `modulePrefix` | Directory prefix used to identify data modules (e.g. `etl-mapwarper`)
| `outputDir` | Directory to which data modules write their data

The configuration of the separate data modules can also be done in configuration file.

## Installation

First, clone this repository and the repository of the data modules you need:

    git clone https://github.com/nypl-spacetime/spacetime-etl.git

    # Data module repositories:
    git clone https://github.com/nypl-spacetime/etl-wards.git
    git clone https://github.com/nypl-spacetime/etl-mapwarper.git
    git clone https://github.com/nypl-spacetime/etl-oldnyc.git

Then, install dependencies:

    cd spacetime-etl
    npm install
    cd ..

    # Data module repositories:    
    cd etl-wards
    npm install
    cd ..
    cd etl-mapwarper
    npm install
    cd ..
    cd etl-oldnyc
    npm install

## Download and convert data

Run the data tool without command line arguments to get a list of the available data modules:

  node index.js

To execute a module, provide their dataset IDs as command line parameters:

    node index.js mapwarper oldnyc ...

Alternatively, you can select the processing steps you want to run:

    node index.js --steps=convert mapwarper

By default, all steps are run consecutively.

Copyright (C) 2015 [Waag Society](http://waag.org),  2016 [The New York Public Library](http://nypl.org)
