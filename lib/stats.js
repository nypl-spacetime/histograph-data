const fuzzyDates = require('fuzzy-dates')

module.exports = function () {
  let stats

  function add (type, obj) {
    if (!stats) {
      stats = {}
    }

    // object => objects, relation => relations, log => logs
    const key = type + 's'

    if (!stats[key]) {
      stats[key] = {
        count: 0
      }
    }

    stats[key].count += 1

    if (obj.type) {
      if (!stats[key].types) {
        stats[key].types = {}
      }

      if (!stats[key].types[obj.type]) {
        stats[key].types[obj.type] = 0
      }
      stats[key].types[obj.type] += 1
    }

    if (type === 'object') {
      const timestamps = []

      if (obj.geometry) {
        if (!stats.objects.geometries) {
          stats.objects.geometries = 0
        }

        stats.objects.geometries += 1
      }

      try {
        if (obj.validSince) {
          timestamps.push(new Date(fuzzyDates.convert(obj.validSince)[0]).getTime())
        }

        if (obj.validUntil) {
          timestamps.push(new Date(fuzzyDates.convert(obj.validUntil)[1]).getTime())
        }
      } catch (err) {
        console.log(err)
      }

      if (timestamps.length) {
        if (!stats.objects.decades) {
          stats.objects.decades = {}
        }

        const timestamp = timestamps.reduce((a, b) => a + b, 0) / timestamps.length
        const decade = Math.floor(new Date(timestamp).getFullYear() / 10) * 10

        if (!stats.objects.decades[decade]) {
          stats.objects.decades[decade] = 0
        }
        stats.objects.decades[decade] += 1
      }
    }
  }

  function getStats () {
    return {
      stats
    }
  }

  return {
    add,
    getStats
  }
}
