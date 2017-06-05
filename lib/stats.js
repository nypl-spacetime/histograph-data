const fuzzyDates = require('fuzzy-dates')

module.exports = function () {
  let stats
  let hasLogs = false

  let emptyStats = {
    objects: {
      count: 0,
      types: {},
      decades: {}
    },
    relations: {
      count: 0,
      types: {}
    }
  }

  function add (type, obj) {
    if (type === 'log') {
      hasLogs = true
      return
    }

    if (!stats) {
      stats = {}
    }

    // object => objects, relation => relations
    const key = type + 's'

    if (!stats[key]) {
      stats[key] = emptyStats[key]
    }

    stats[key].count += 1

    if (!stats[key].types[obj.type]) {
      stats[key].types[obj.type] = 0
    }
    stats[key].types[obj.type] += 1

    if (type === 'object') {
      var timestamps = []

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
      hasLogs,
      stats
    }
  }

  return {
    add,
    getStats
  }
}
