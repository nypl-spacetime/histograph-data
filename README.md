# Histograph Data

Scripts to download and convert default data sources to Histograph format. Configuration of the individual scripts can be done in the [Histograph configuration file](https://github.com/histograph/config).

First, install dependencies:

    $ npm install

Then, you can run all source processing scripts defined in the configuration file like this:

    $ node index.js

Or you can select the scripts you want to run yourself:

    $ node index.js geonames tgn ...

Alternatively, you can select the processing steps you want to run:

    $ node index.js --steps=convert tgn geonames

Valid processing steps:

1. `download`
2. `convert`

By default, all steps are run consecutively.

## Data sources

- [GeoNames](http://www.geonames.org/)
- [Getty Thesaurus of Geographic Names](http://www.getty.edu/research/tools/vocabularies/tgn/)
- [Basisregistraties Adressen en gebouwen](http://www.basisregistratiesienm.nl/basisregistraties/adressen-en-gebouwen)
- [Nationaal Wegenbestand](https://data.overheid.nl/data/dataset/nationaal-wegen-bestand-wegen-hectopunten)
- [CShapes](http://nils.weidmann.ws/projects/cshapes)

## License

Copyright (C) 2015 [Waag Society](http://waag.org).

The source for Histograph is released under the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
