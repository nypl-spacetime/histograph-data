const etl = require('../')

etl('nyc-wards', (err) => {
  if (err) {
    console.error('Error:')
    console.error(err)
  } else {
    console.log('Done!')
  }
})
