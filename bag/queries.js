var urlify = require('urlify').create({
  addEToUmlauts: true,
  szToSs: true,
  toLower: true,
  spaces: '-',
  nonPrintable: '-',
  trim: true
});

module.exports = [
  {
    name: 'openbareruimte',
    rowToPitsAndRelations: function(row) {
      var pit = {
        id: parseInt(row.id),
        name: row.name,
        type: 'hg:Street',
        data: {
          woonplaatscode: parseInt(row.woonplaatscode),
          woonplaatsnaam: row.woonplaatsnaam
        }
      };

      var woonplaatsRelation = {
        from: parseInt(row.id),
        to: parseInt(row.woonplaatscode),
        type: 'hg:liesIn'
      };

      var nwbId = 'nwb/' + urlify(row.woonplaatsnaam + '-' + row.name);

      var nwbRelation = {
        from: parseInt(row.id),
        to: nwbId,
        type: 'hg:sameHgConcept'
      };

      return [
        {
          type: 'pits',
          obj: pit
        },
        {
          type: 'relations',
          obj: woonplaatsRelation
        },
        {
          type: 'relations',
          obj: nwbRelation
        }
      ];
    }
  },

  {
    name: 'woonplaats',
    rowToPitsAndRelations: function(row) {
      var pit = {
        id: parseInt(row.id),
        name: row.name,
        type: 'hg:Place',
        geometry: JSON.parse(row.geometry),
        data: {
          gemeentecode: parseInt(row.gemeentecode)
        }
      };

      return [
        {
          type: 'pits',
          obj: pit
        }
      ];
    }
  },

  {
    name: 'pand',
    rowToPitsAndRelations: function(row) {
      var pit = {
        id: parseInt(row.id),
        type: 'hg:Building',
        validSince: row.bouwjaar,
        geometry: JSON.parse(row.geometry)
      };

      var result = [
        {
          type: 'pits',
          obj: pit
        }
      ];

      if (row.openbareruimtes) {
        row.openbareruimtes.split(',').forEach(function(openbareruimte) {
          result.push({
            type: 'relations',
            obj: {
              from: parseInt(row.id),
              to: parseInt(openbareruimte),
              type: 'hg:liesIn'
            }
          });
        });
      }

      return [
        {
          type: 'pits',
          obj: pit
        }
      ];
    }
  },

  {
    name: 'nummeraanduiding',
    rowToPitsAndRelations: function(row) {
      var pit = {
        id: parseInt(row.id),
        name: [row.openbareruimtenaam, row.huisnummer, row.huisletter, row.huisnummertoevoeging].filter(function(p) {
            return p;
          }).join(' '),
        type: 'hg:Address',
        geometry: JSON.parse(row.geometry),
        data: {
          openbareruimte: parseInt(row.openbareruimte),
          postcode: row.postcode
        }
      };

      var relation = {
        from: parseInt(row.id),
        to: parseInt(row.openbareruimte),
        type: 'hg:liesIn'
      };

      var result = [
        {
          type: 'pits',
          obj: pit
        },
        {
          type: 'relations',
          obj: relation
        }
      ];

      if (row.pand_ids) {
        row.pand_ids.split(',').forEach(function(pandId) {
          result.push({
            type: 'relations',
            obj: {
              from: parseInt(row.id),
              to: parseInt(pandId),
              type: 'hg:liesIn'
            }
          });
        });
      }

      return result;
    }
  }
];
