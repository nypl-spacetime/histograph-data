var fs = require('fs');
var path = require('path');
var SparqlClient = require('sparql-client');
var async = require('async');
var xml2js = require('xml2js');

var pitsAndRelations;

// TGN configuration
var sparqlFiles = [
  'tgn-parents',
  'tgn-places',
  'tgn-terms'
];

var types = {
  nations: 'Country',
  area: 'Area',
  canals: 'Water',
  channels: 'Water',
  'general regions': 'Region',
  'inhabited places': 'Place',
  provinces: 'Province',
  'second level subdivisions': 'Region',
  neighborhoods: 'Neighbourhood'
};

var sparqlEndpoint = 'http://vocab.getty.edu/sparql.rdf';

exports.download = function(config, callback) {
  async.eachSeries(sparqlFiles, function(sparqlFile, callback) {
    async.eachSeries(config.parents, function(parent, callback) {
      var client = new SparqlClient(sparqlEndpoint);
      var sparqlQuery = fs.readFileSync(path.join(__dirname, sparqlFile + '.sparql'), 'utf8');

      sparqlQuery = sparqlQuery.replace(new RegExp('{{ parent }}', 'g'), parent);

      client.query(sparqlQuery)
        .execute(function(error, results) {
          fs.writeFileSync(path.join(__dirname, sparqlFile + '.' +  parent.replace('tgn:', '') + '.xml'), results);
          callback();
        });
    },

    function(err) {
      callback(err);
    });
  },

  function(err) {
    callback(err);
  });
};

exports.convert = function(config, callback) {
  pitsAndRelations = require('../pits-and-relations')({
    source: 'tgn',
    truncate: true
  });

  var parser = new xml2js.Parser();

  async.eachSeries(sparqlFiles, function(sparqlFile, callback) {
    async.eachSeries(config.parents, function(parent, callback) {

      fs.readFile(path.join(__dirname, sparqlFile + '.' + parent.replace('tgn:', '') + '.xml'), function(err, data) {
        parser.parseString(data, function(err, result) {
          async.eachSeries(result['rdf:RDF']['rdf:Description'], function(element, callback) {
            parseElement(element, function(err) {
              callback(err);
            });
          },

          function(err) {
            callback(err);
          });

        });
      });

    },

    function(err) {
      callback(err);
    });
  },

  function(err) {
    callback(err);
  });

  function getElementTagValue(element, tag) {
    if (element[tag] && element[tag].length > 0 && element[tag][0]._) {
      return element[tag][0]._;
    } else if (element[tag] && element[tag].length > 0) {
      return element[tag][0];
    }

    return null;
  }

  function getElementTagAttribute(element, tag, attribute) {
    if (element[tag] && element[tag].length > 0 && element[tag][0].$ && element[tag][0].$[attribute]) {
      return element[tag][0].$[attribute];
    }

    return null;
  }

  function parseElement(element, callback) {
    var elementType = getElementTagValue(element, 'tgn:typeTerm');
    var type = types[elementType];

    // Only process elements with valid type
    if (type) {
      var emit = [];

      var name = getElementTagValue(element, 'gvp:term');
      var uri = getElementTagAttribute(element, 'dct:source', 'rdf:resource');

      var pit = {
        uri: uri,
        name: name,
        type: type
      };

      var long = getElementTagValue(element, 'wgs:long');
      var lat = getElementTagValue(element, 'wgs:lat');
      if (long && lat) {
        pit.geometry = {
          type: 'Point',
          coordinates: [
            parseFloat(long),
            parseFloat(lat)
          ]
        };
      }

      var comment = getElementTagValue(element, 'rdfs:comment');
      if (comment) {
        pit.data = {
          comment: comment
        };
      }

      // TODO: use just years, and specify fuzziness!
      var estStart = getElementTagValue(element, 'gvp:estStart');
      var estEnd = getElementTagValue(element, 'gvp:estEnd');
      if (estStart) {
        pit.hasBeginning = estStart + '-01-01';
      }

      if (estEnd) {
        pit.hasEnd = estEnd + '-12-31';
      }

      emit.push({
        type: 'pits',
        obj: pit
      });

      var broaderPreferred = getElementTagAttribute(element, 'gvp:broaderPreferred', 'rdf:resource');
      if (broaderPreferred) {
        // This implies that current PIT lies in broaderPreferred
        // Add liesIn relation
        emit.push({
          type: 'relations',
          obj: {
            from: uri,
            to: broaderPreferred,
            type: 'liesIn'
          }
        });
      }

      var subject = getElementTagAttribute(element, 'rdf:subject', 'rdf:resource');
      if (subject) {
        // This implies that subject is an alternative name for current PIT
        // Add sameHgConcept relation
        emit.push({
          type: 'relations',
          obj: {
            from: uri,
            to: subject,
            type: 'sameHgConcept'
          }
        });
      }

      async.eachSeries(emit, function(item, callback) {
        pitsAndRelations.emit(item.type, item.obj, function(err) {
          callback(err);
        });
      },

      function(err) {
        callback(err);
      });
    } else {
      setImmediate(callback);
    }
  }
};

exports.done = function(config, callback) {
  if (pitsAndRelations) {
    pitsAndRelations.close();
  }

  callback();
};
