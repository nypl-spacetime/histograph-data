var fs = require('fs');
var path = require('path');
var SparqlClient = require('sparql-client');
var async = require('async');
var xml2js = require('xml2js');
var pitsAndRelations = require('../pits-and-relations')({
  source: 'tgn',
  truncate: true
});

// TGN configuration
var sparqlFiles = [
  'tgn-parents',
  'tgn-places',
  'tgn-terms'
];

var types = {
  'nations': 'hg:Country',
  'area': 'hg:Area',
  'canals': 'hg:Water',
  'channels': 'hg:Water',
  'general regions': 'hg:Region',
  'inhabited places': 'hg:Place',
  'provinces': 'hg:Province',
  'second level subdivisions': 'hg:Region',
  'neighborhoods': 'hg:Neighbourhood'
};

var sparqlEndpoint = 'http://vocab.getty.edu/sparql.rdf';

exports.download = function(config, callback) {
  async.eachSeries(sparqlFiles, function(sparqlFile, callback) {
    async.eachSeries(config.args, function(args, callback) {
      var client = new SparqlClient(sparqlEndpoint);
      var sparqlQuery = fs.readFileSync(path.join(__dirname, sparqlFile + '.sparql'), 'utf8');

      Object.keys(args).forEach(function(arg) {
        sparqlQuery = sparqlQuery.replace(new RegExp('{{ ' + arg + ' }}', 'g'), args[arg]);
      });

      client.query(sparqlQuery)
        .execute(function(error, results) {
          fs.writeFileSync(path.join(__dirname, sparqlFile + '.' + args.parent.replace('tgn:', '') + '.xml'), results);
          callback();
        });
    },

    function(err) {
      callback(err);
    });
  }, function(err) {
    callback(err);
  });
};

exports.convert = function(config, callback) {
  var parser = new xml2js.Parser();

  async.eachSeries(sparqlFiles, function(sparqlFile, callback) {
    async.eachSeries(config.args, function(args, callback) {

      fs.readFile(path.join(__dirname, sparqlFile + '.' + args.parent.replace('tgn:', '') + '.xml'), function(err, data) {
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
    if (element[tag] && element[tag].length > 0 && element[tag][0]['_']) {
      return element[tag][0]['_'];
    } else if (element[tag] && element[tag].length > 0) {
      return element[tag][0];
    }
    return null;
  }

  function getElementTagAttribute(element, tag, attribute) {
    if (element[tag] && element[tag].length > 0 && element[tag][0]['$'] && element[tag][0]['$'][attribute]) {
      return element[tag][0]['$'][attribute];
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
      var id = uri.replace('http://vocab.getty.edu/tgn/', '').replace('/', '-');

      var pit = {
        id: id,
        name: name,
        type: type,
        uri: uri
      };

      var long = getElementTagValue(element, 'wgs:long');
      var lat = getElementTagValue(element, 'wgs:lat');
      if (long && lat) {
        pit.geometry = {
          type: "Point",
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

      // var estStart = getElementTagValue(element, 'gvp:estStart');
      // var estEnd = getElementTagValue(element, 'gvp:estEnd');
      // if (estStart) {
      //   pit.hasBeginning = estStart;
      // }
      // if (estEnd) {
      //   pit.hasEnd = estEnd;
      // }

      emit.push({
        type: 'pits',
        obj: pit
      });

      var broaderPreferred = getElementTagAttribute(element, 'gvp:broaderPreferred', 'rdf:resource');
      if (broaderPreferred) {
        // This implies that current PIT lies in broaderPreferred
        // Add hg:within relation
        emit.push({
          type: 'relations',
          obj: {
            from: id,
            to: broaderPreferred,
            label: 'hg:within'
          }
        });
      }

      var subject = getElementTagAttribute(element, 'rdf:subject', 'rdf:resource');
      if (subject) {
        // This implies that subject is an alternative name for current PIT
        // Add hg:sameHgConcept relation
        emit.push({
          type: 'relations',
          obj: {
            from: uri,
            to: subject,
            label: 'hg:sameHgConcept'
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
  pitsAndRelations.close();
  callback();
}
