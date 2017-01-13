'use strict'

let Promise = require('bluebird')
let EventEmitter = require('events').EventEmitter
let async = require('async')
let isEmpty = require('lodash.isempty')
let Broker = require('../lib/broker.lib')
let inputPipes = process.env.INPUT_PIPES.split(',')
let outputPipes = process.env.OUTPUT_PIPES.split(',')
let loggers = process.env.LOGGERS.split(',')
let exceptionLoggers = process.env.EXCEPTION_LOGGERS.split(',')

class Service extends EventEmitter {
  constructor () {
    super()

    let dataEmitter = (data) => {
      async.waterfall([
        async.constant(data.content.toString('utf8')),
        async.asyncify(JSON.parse)
      ], (err, parsed) => {
        if (err) return console.error(err)

        this.emit('data', parsed)
      })
    }

    this.queues = []
    this._broker = new Broker()
    let broker = this._broker

    loggers.push('generic.logs')
    exceptionLoggers.push('generic.exceptions')

    async.waterfall([
      (done) => {
        async.waterfall([
          async.constant(process.env.CONFIG),
          async.asyncify(JSON.parse)
        ], (err, parsed) => {
          done(err)
          this.config = parsed
        })
      },
      (done) => {
        // connect to rabbitmq
        broker.connect(process.env.BROKER)
          .then(() => {
            console.log('Connected to RabbitMQ Server.')
            done()
          })
          .catch((error) => {
            done(error)
          })
      },
      (done) => {
        let queueIds = inputPipes
          .concat(outputPipes)
          .concat(loggers)
          .concat(exceptionLoggers)

        async.each(queueIds, (queueID, callback) => {
          broker.newQueue(queueID)
            .then((queue) => {
              this.queues[queueID] = queue
              callback()
            })
            .catch((error) => {
              callback(error)
            })
        }, (error) => {
          if (!error) console.log('Connected to queues.')
          done(error)
        })
      },
      (done) => {
        // consume input pipes
        async.each(inputPipes, (inputPipe, callback) => {
          this.queues[inputPipe].consume((msg) => {
            dataEmitter(msg)
          })
            .then(() => {
              callback()
            })
            .catch((error) => {
              callback(error)
            })
        }, (error) => {
          if (!error) console.log('Input pipes consumed.')
          done(error)
        })
      }
    ], (error) => {
      if (error) return console.error(error)

      console.log('Plugin init process done.')
      this.emit('ready')
    })
  }

  log (logData) {
    return new Promise((resolve, reject) => {
      if (isEmpty(logData)) return reject(new Error(`Please specify a data to log.`))

      async.each(loggers, (logger, done) => {
        this.queues[logger].publish(logData)
          .then(() => {
            console.log(`message written to queue ${logger}`)
            done()
          })
          .catch((error) => {
            done(error)
          })
      }, (error) => {
        if (error) return reject(error)
        resolve()
      })
    })
  }

  logException (err) {
    let errData = {
      name: err.name,
      message: err.message,
      stack: err.stack
    }

    return new Promise((resolve, reject) => {
      async.each(exceptionLoggers, (logger, done) => {
        this.queues[logger].publish(errData)
          .then(() => {
            console.log(`message written to queue ${logger}`)
            done()
          })
          .catch((error) => {
            done(error)
          })
      }, (error) => {
        if (error) return reject(error)
        resolve()
      })
    })
  }

  pipe (data, result) {
    return new Promise((resolve, reject) => {
      if(isEmpty(data) || isEmpty(result)) return reject(new Error('Please specify the original data and the result.'))

      if (process.env.OUTPUT_SCHEME === 'MERGE') {
        let outputData = JSON.stringify(Object.assign(data, result))

        async.each(outputPipes, (outputPipe, done) => {
          this.queues[outputPipe].publish(outputData)
            .then(() => {
              console.log(`message written to queue ${outputPipe}`)
              done()
            })
            .catch((error) => {
              done(error)
            })
        }, (error) => {
          if (error) return reject(error)
          resolve()
        })
      }
      else if (process.env.OUTPUT_SCHEME === 'NAMESPACE') {
        let outputData = JSON.stringify(Object.assign(data[process.env.OUTPUT_NAMESPACE], result))

        async.each(outputPipes, (outputPipe, done) => {
          this.queues[outputPipe].publish(outputData)
            .then(() => {
              console.log(`message written to queue ${outputPipe}`)
              done()
            })
            .catch((error) => {
              done(error)
            })
        }, (error) => {
          if (error) return reject(error)
          resolve()
        })
      }
      else if (process.env.OUTPUT_SCHEME === 'RESULT') {
        let outputData = JSON.stringify(result)

        async.each(outputPipes, (outputPipe, done) => {
          this.queues[outputPipe].publish(outputData)
            .then(() => {
              console.log(`message written to queue ${outputPipe}`)
              done()
            })
            .catch((error) => {
              done(error)
            })
        }, (error) => {
          if (error) return reject(error)
          resolve()
        })
      }
    })
  }
}

module.exports = Service