module.exports = [
  {
    name: 'pand',
    rowToPitsAndRelations: function(row) {
      var pit = {
        id: parseInt(row.id),
        type: 'hg:Building',
        data: {
          bouwjaar: parseInt(row.bouwjaar)
        },
        geometry: JSON.parse(row.geometry)
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
    name: 'openbareruimte',
    rowToPitsAndRelations: function(row) {
      var pit = {
        id: parseInt(row.id),
        name: row.name,
        type: 'hg:Street',
        data: {
          woonplaatscode: parseInt(row.woonplaatscode)
        }
      };

      var relation = {
        from: parseInt(row.id),
        to: parseInt(row.woonplaatscode),
        label: 'hg:liesIn'
      };

      return [
        {
          type: 'pits',
          obj: pit
        },
        {
          type: 'relations',
          obj: relation
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
    name: 'verblijfsobject',
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
        label: 'hg:liesIn'
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
              label: 'hg:liesIn'
            }
          });
        });
      }

      return result;
    }
  }
];
