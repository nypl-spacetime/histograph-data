# Histograph Data

Scripts to download and convert default data sources to Histograph format. Configuration of the individual scripts can be done in the [Histograph configuration file](https://github.com/histograph/config).

First, install dependencies:

    $ npm install

Then, you can run all source processing scripts defined in the configuration file like this:

    $ node index.js

Or you can select the scripts you want to run yourself:

    $ node index.js geonames tgn ...

Alternatively, you can select the processing steps you want to run:

    $ node index.js --steps=convert,infer tgn geonames

Valid processing steps:

- `download`
- `convert`
- `infer`

## Data sources

- [GeoNames](http://www.geonames.org/)
- [Getty Thesaurus of Geographic Names](http://www.getty.edu/research/tools/vocabularies/tgn/)
- [Basisregistraties Adressen en gebouwen](http://www.basisregistratiesienm.nl/basisregistraties/adressen-en-gebouwen)
