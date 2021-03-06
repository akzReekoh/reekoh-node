'use strict'

const async = require('async')
const BPromise = require('bluebird')
const EventEmitter = require('events').EventEmitter

const isEmpty = require('lodash.isempty')
const isError = require('lodash.iserror')
const isString = require('lodash.isstring')
const isPlainObject = require('lodash.isplainobject')

const Broker = require('../broker.lib')

class Connector extends EventEmitter {
  constructor () {
    super()

    const BROKER = process.env.BROKER
    const ACCOUNT = process.env.ACCOUNT
    const INPUT_PIPE = process.env.INPUT_PIPE

    const LOGGERS = `${process.env.LOGGERS || ''}`.split(',').filter(Boolean)
    const EXCEPTION_LOGGERS = `${process.env.EXCEPTION_LOGGERS || ''}`.split(',').filter(Boolean)

    let _broker = new Broker()

    process.env.ACCOUNT = undefined
    process.env.INPUT_PIPE = undefined
    process.env.LOGGERS = undefined
    process.env.EXCEPTION_LOGGERS = undefined
    process.env.BROKER = undefined

    async.series([
      (done) => {
        async.waterfall([
          async.constant(process.env.CONFIG || '{}'),
          async.asyncify(JSON.parse)
        ], (err, parsed) => {
          if (!err) {
            this.config = parsed
            process.env.CONFIG = undefined
          }

          done(err)
        })
      },
      (done) => {
        // connect to rabbitmq
        _broker.connect(BROKER).then(() => {
          console.log('Connected to RabbitMQ Server.')
          done()
        }).catch(done)
      },
      (done) => {
        let queueIds = [INPUT_PIPE]
          .concat(LOGGERS)
          .concat(EXCEPTION_LOGGERS)
          .concat(['logs', 'exceptions'])

        async.each(queueIds, (queueID, cb) => {
          _broker.createQueue(queueID).then(() => {
            cb()
          }).catch(cb)
        }, (err) => {
          if (!err) console.log('Connected to queues.')
          done(err)
        })
      },
      (done) => {
        // consume input pipe
        _broker.queues[INPUT_PIPE].consume((msg) => {
          async.waterfall([
            async.constant(msg.content.toString('utf8')),
            async.asyncify(JSON.parse)
          ], (err, parsed) => {
            if (err) return console.error(err)

            this.emit('data', parsed)
          })
        }).then(() => {
          console.log('Input pipe consumed.')
          done()
        }).catch(done)
      }
    ], (err) => {
      if (err) {
        console.error(err)
        throw err
      }

      process.nextTick(() => {
        console.log('Plugin init process done.')
        this.emit('ready')
      })
    })

    this.log = (logData) => {
      return new BPromise((resolve, reject) => {
        if (isEmpty(logData)) return reject(new Error(`Please specify a data to log.`))

        if (!isPlainObject(logData) && !isString(logData)) return reject(new Error('Log data must be a string or object'))

        async.parallel([
          (callback) => {
            async.each(LOGGERS, (logger, done) => {
              _broker.queues[logger].publish(logData).then(() => {
                console.log(`message written to queue ${logger}`)
                done()
              }).catch(done)
            }, callback)
          },
          (callback) => {
            let data = {
              account: ACCOUNT,
              data: logData
            }

            _broker.queues['logs'].publish(data).then(() => {
              console.log(`message written to queue logs`)
              callback()
            }).catch(callback)
          }
        ], (err) => {
          if (err) return reject(err)

          resolve()
        })
      })
    }

    this.logException = (err) => {
      return new BPromise((resolve, reject) => {
        if (!isError(err)) return reject(new Error('Please specify a valid error to log.'))

        let errData = {
          name: err.name,
          message: err.message,
          stack: err.stack
        }

        async.parallel([
          (callback) => {
            async.each(EXCEPTION_LOGGERS, (logger, done) => {
              _broker.queues[logger].publish(errData).then(() => {
                console.log(`message written to queue ${logger}`)
                done()
              }).catch(done)
            }, callback)
          },
          (callback) => {
            let data = {
              account: ACCOUNT,
              data: errData
            }

            _broker.queues['exceptions'].publish(data).then(() => {
              console.log(`message written to queue exceptions`)
              callback()
            }).catch(callback)
          }
        ], (err) => {
          if (err) return reject(err)

          resolve()
        })
      })
    }
  }
}

module.exports = Connector
