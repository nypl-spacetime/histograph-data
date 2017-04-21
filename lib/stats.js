const edtf = require('edtf')

module.exports = function () {
  let stats = {
    hasLogs: false
  }

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

  let empty = true

  function add (type, obj) {
    empty = false

    if (type === 'log') {
      stats.hasLogs = true
      return
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
          timestamps.push(edtf(String(obj.validSince)).min)
        }

        if (obj.validUntil) {
          timestamps.push(edtf(String(obj.validUntil)).max)
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
    if (!empty) {
      return stats
    }
  }

  return {
    add,
    getStats
  }
}
