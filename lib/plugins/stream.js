'use strict'

const uuid = require('uuid/v4')
const async = require('async')
const BPromise = require('bluebird')
const NodeCache = require('node-cache')
const EventEmitter = require('events').EventEmitter

const isNil = require('lodash.isnil')
const isError = require('lodash.iserror')
const isEmpty = require('lodash.isempty')
const isString = require('lodash.isstring')
const isPlainObject = require('lodash.isplainobject')

const Broker = require('../broker.lib')

class Stream extends EventEmitter {
  constructor () {
    super()

    const BROKER = process.env.BROKER
    const ACCOUNT = process.env.ACCOUNT
    const COMMAND_RELAYS = `${process.env.COMMAND_RELAYS || ''}`.split(',').filter(Boolean)
    const PLUGIN_ID = process.env.PLUGIN_ID

    const OUTPUT_PIPES = `${process.env.OUTPUT_PIPES || ''}`.split(',').filter(Boolean)

    const LOGGERS = `${process.env.LOGGERS || ''}`.split(',').filter(Boolean)
    const EXCEPTION_LOGGERS = `${process.env.EXCEPTION_LOGGERS || ''}`.split(',').filter(Boolean)

    let _broker = new Broker()
    let _cmdCache = new NodeCache({stdTTL: 60 * 5})
    let _outputPipes = process.env.OUTPUT_PIPES

    process.env.ACCOUNT = undefined
    process.env.PLUGIN_ID = undefined
    process.env.COMMAND_RELAYS = undefined
    process.env.OUTPUT_PIPES = undefined
    process.env.LOGGERS = undefined
    process.env.EXCEPTION_LOGGERS = undefined
    process.env.BROKER = undefined

    async.series([
      (done) => {
        async.waterfall([
          async.constant(process.env.CONFIG),
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
        // initialise RPC
        async.each(['deviceinfo', 'plugin.state.rpc'], (iRPC, cb) => {
          _broker.createRPC('client', iRPC)
            .then((queue) => {
              return queue.consume()
            })
            .then(() => {
              return cb() || null
            })
            .catch(cb)
        }, done)
      },
      (done) => {
        let queueIds = OUTPUT_PIPES
          .concat(LOGGERS)
          .concat(EXCEPTION_LOGGERS)
          .concat(['devices', 'data', 'cmd.responses', 'plugin.state', PLUGIN_ID])
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
        //  create exchange queue
        async.each(COMMAND_RELAYS, (commandRelay, cb) => {
          _broker.createExchange(`${commandRelay}.topic`).then((exchange) => {
            console.log(`${commandRelay}.topic exchange created.`)

            exchange.consume((msg) => {
              async.waterfall([
                async.constant(msg.content.toString('utf8')),
                async.asyncify(JSON.parse)
              ], (err, parsed) => {
                if (err) return console.error('Stream exchange data recevied is not a valid JSON', err)

                if (!_cmdCache.get(parsed.sequenceId)) {
                  _cmdCache.set(parsed.sequenceId, '0')

                  delete parsed.sequenceId
                  return this.emit('command', parsed)
                }
              })
            })
            cb()
          }).catch(cb)
        }, done)
      },
      (done) => {
        _broker.queues[PLUGIN_ID].consume(() => {
          this.emit('sync')
        })

        done()
      }
    ], (err) => {
      if (err) {
        console.error('Stream: ', err)
        throw err
      }

      process.nextTick(() => {
        // plugin initialized
        this.emit('ready')
      })
    })

    this.pipe = (data, sequenceId) => {
      return new BPromise((resolve, reject) => {
        if (isEmpty(data) || !isPlainObject(data)) return reject(new Error('Invalid data received. Data should be an Object and should not be empty.'))

        if (sequenceId && !isString(sequenceId)) return reject(new Error('Kindly specify a valid sequence id'))

        if (sequenceId) {
          _broker.queues['data'].publish({
            plugin: PLUGIN_ID,
            pipe: _outputPipes,
            sequenceId: sequenceId,
            data: data
          }).then(() => {
            console.log('data written to queue data')
            resolve()
          }).catch(reject)
        } else {
          async.each(OUTPUT_PIPES, (outputPipe, done) => {
            _broker.queues[outputPipe].publish(data).then(() => {
              console.log(`data written to queue ${outputPipe}`)
              done()
            }).catch(done)
          }, (err) => {
            if (err) return reject(err)

            resolve()
          })
        }
      })
    }

    this.sendCommandResponse = (commandId, response) => {
      return new BPromise((resolve, reject) => {
        if (!isString(commandId) || isEmpty(commandId)) return reject(new Error('Invalid Command ID received. Command ID should be a string and should not be empty.'))

        if (!isString(response) || isEmpty(response)) return reject(new Error('Invalid Response received. Response should be a string and should not be empty.'))

        _broker.queues['cmd.responses'].publish({
          commandId: commandId,
          response: response,
          account: ACCOUNT
        }).then(() => {
          console.log('data written to queue cmd.responses')
          resolve()
        }).catch(reject)
      })
    }

    this.notifyConnection = (deviceId) => {
      return new BPromise((resolve, reject) => {
        if (isEmpty(deviceId)) return reject(new Error('Please specify the device identifier.'))

        if (!isString(deviceId)) return reject(new Error('DeviceID should be a string.'))

        _broker.queues['devices'].publish({
          data: {
            operation: 'connect',
            device: {
              _id: deviceId
            }
          },
          account: ACCOUNT
        }).then(() => {
          console.log('data written to queue devices')
          resolve()
        }).catch(reject)
      })
    }

    this.notifyDisconnection = (deviceId) => {
      return new BPromise((resolve, reject) => {
        if (isEmpty(deviceId)) return reject(new Error('Please specify the device identifier.'))

        if (!isString(deviceId)) return reject(new Error('DeviceID should be a string.'))

        _broker.queues['devices'].publish({
          data: {
            operation: 'disconnect',
            device: {
              _id: deviceId
            }
          },
          account: ACCOUNT
        }).then(() => {
          console.log('data written to queue devices')
          resolve()
        }).catch(reject)
      })
    }

    this.setDeviceState = (deviceId, state) => {
      return new BPromise((resolve, reject) => {
        if (isEmpty(deviceId)) return reject(new Error('Please specify the device identifier.'))

        if (!isString(deviceId)) return reject(new Error('DeviceID should be a string.'))

        if (isEmpty(state) || isNil(state)) return reject(new Error('Please specify the device state.'))

        _broker.queues['devices'].publish({
          data: {
            operation: 'setstate',
            device: {
              _id: deviceId,
              state: state
            }
          },
          account: ACCOUNT
        }).then(() => {
          console.log('data written to queue devices')
          resolve()
        }).catch(reject)
      })
    }

    this.requestDeviceInfo = (deviceId) => {
      return new BPromise((resolve, reject) => {
        if (isEmpty(deviceId)) return reject(new Error('Please specify the device identifier.'))

        if (!isString(deviceId)) return reject(new Error('DeviceID should be a string.'))

        let requestId = uuid()

        _broker.rpcs['deviceinfo'].once(requestId, (deviceInfo) => {
          resolve(deviceInfo)
        })

        _broker.rpcs['deviceinfo'].publish(requestId, {
          account: ACCOUNT,
          device: {
            _id: deviceId
          }
        }).then(() => {
          // Do nothing
        }).catch(reject)
      }).timeout(10000, 'Request for device information has timed out.')
    }

    this.setState = (state) => {
      return new BPromise((resolve, reject) => {
        if (isNil(state)) return reject(new Error(`Please specify a state to set.`))

        let data = {
          state: state,
          plugin: PLUGIN_ID
        }

        _broker.queues['plugin.state'].publish(data)
          .then(() => {
            console.log(`message written to queue plugin.state`)
            resolve()
          })
          .catch(reject)
      })
    }

    this.getState = () => {
      return new BPromise((resolve, reject) => {
        let requestId = uuid()

        _broker.rpcs['plugin.state.rpc'].once(requestId, (state) => {
          resolve(state)
        })

        _broker.rpcs['plugin.state.rpc'].publish(requestId, {
          plugin: PLUGIN_ID
        }).then(() => {
          // Do nothing
        }).catch(reject)
      }).timeout(10000, 'Request for plugin state has timed out.')
    }

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

module.exports = Stream
