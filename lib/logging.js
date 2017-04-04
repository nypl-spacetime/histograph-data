const path = require('path')
const chalk = require('chalk')
const moment = require('moment')

const PADDING = '     '

function logModulesStart (config) {
  console.log('Using ETL modules in ' + chalk.underline(`${path.join(config.etl.moduleDir, config.etl.modulePrefix)}*`))
  console.log(chalk.gray(`  Saving data to ${chalk.underline(path.join(config.etl.outputDir, '<step>'))}\n`))
}

function logModuleTitle (module) {
  let gray = []

  if (module.dataset) {
    if (module.dataset.title) {
      gray.push(module.dataset.title)
    }

    if (module.dataset.website) {
      gray.push(chalk.underline(module.dataset.website))
    }
  }

  console.log(` - ${module.id} ${chalk.gray(gray.join(' - '))}`)
}

function logModule (module, modulePath) {
  logModuleTitle(module)

  if (module.err) {
    logModuleError(module, modulePath)
  } else {
    logModuleSteps(module)
  }

  return module
}

function logModuleSteps (module) {
  const stepsLabel = 'steps: '
  const stepsFirst = chalk.gray(stepsLabel)
  const stepsOther = new Array(stepsLabel.length + 1).join(' ')

  if (module.steps && module.steps.length) {
    module.steps.forEach((step, i) => {
      const status = module.status[step]

      const getStatusText = (text) => `${chalk.gray('(')}${text}${chalk.gray(')')}`
      let statusText

      if (status) {
        const fromNow = chalk.bold(moment(status.date).fromNow())
        const success = status.success ? chalk.green('success') : chalk.red('failed')
        statusText = getStatusText(chalk.gray(`last ran ${fromNow}: ${success}`))
      } else {
        statusText = getStatusText(chalk.yellow('never ran'))
      }
      console.log(`${PADDING}${i === 0 ? stepsFirst : stepsOther}${chalk.blue(step)} ${statusText}`)
    })
  } else {
    console.log(`${PADDING}${stepsFirst}${chalk.red('no steps found!')}`)
  }
}

function logErrorStack (err, padding = '') {
  if (err && err.stack) {
    const stack = err.stack
      .split('\n')
      .map((line) => padding + line)
      .join('\n')
    console.error(chalk.gray(stack))
  }
}

function logModuleError (module, modulePath) {
  let moduleNotFound = (module.err.code === 'MODULE_NOT_FOUND')

  if (moduleNotFound) {
    // See if module itself is not found, or a module required by the module!
    moduleNotFound = module.err.message.includes(modulePath)
  }

  if (moduleNotFound) {
    console.error(PADDING + chalk.red('No valid ETL module: ') + module.id)
  } else {
    console.error(PADDING + chalk.red('Error loading ETL module: ') + module.id)
  }

  logErrorStack(module.err, PADDING)
}

function logModulesEnd (count) {
  if (!count) {
    console.log(chalk.red('No ETL modules found...'))
  }

  console.log('\nUsage: spacetime-etl [--config /path/to/config.yml] [module[.step] ...]')
}

function stepStart (step) {
  console.log(`    ${chalk.gray('executing:')} ${chalk.blue(step)}`)
}

function stepDone (err) {
  console.log(`    ${chalk.gray('result:')} ${err ? chalk.red('error') : chalk.green('success')} ${err ? chalk.gray(err.message || err.stack || err) : ''}`)
}

function stepsDone (err) {
  if (err) {
    console.error(chalk.red('Done, with errors...'))
    logErrorStack(err)
  } else {
    console.log('Done...')
  }
}

module.exports = {
  logModulesStart,
  logModule,
  logModuleTitle,
  logModulesEnd,
  stepStart,
  stepDone,
  stepsDone
}
