'use strict'

const fs = require('fs')
const path = require('path')
const uuid = require('uuid/v4')
const async = require('async')
const BPromise = require('bluebird')
const NodeCache = require('node-cache')
const EventEmitter = require('events').EventEmitter

const isNil = require('lodash.isnil')
const hasProp = require('lodash.has')
const isError = require('lodash.iserror')
const isEmpty = require('lodash.isempty')
const isString = require('lodash.isstring')
const isPlainObject = require('lodash.isplainobject')

const Broker = require('../broker.lib')

class Gateway extends EventEmitter {

  constructor () {
    super()
    this.config = {}
    this.port = process.env.PORT || 8080

    const BROKER = process.env.BROKER
    const ACCOUNT = process.env.ACCOUNT
    const PLUGIN_ID = process.env.PLUGIN_ID

    const LOGGERS = `${process.env.LOGGERS || ''}`.split(',').filter(Boolean)
    const OUTPUT_PIPES = `${process.env.OUTPUT_PIPES || ''}`.split(',').filter(Boolean)
    const COMMAND_RELAYS = `${process.env.COMMAND_RELAYS || ''}`.split(',').filter(Boolean)
    const EXCEPTION_LOGGERS = `${process.env.EXCEPTION_LOGGERS || ''}`.split(',').filter(Boolean)

    let _broker = new Broker()
    let _cmdCache = new NodeCache({stdTTL: 60 * 5})

    process.env.PORT = undefined
    process.env.BROKER = undefined
    process.env.ACCOUNT = undefined
    process.env.LOGGERS = undefined
    process.env.PLUGIN_ID = undefined
    process.env.OUTPUT_PIPES = undefined
    process.env.COMMAND_RELAYS = undefined
    process.env.EXCEPTION_LOGGERS = undefined

    async.series([
      // connecting to rabbitMQ
      (done) => {
        _broker.connect(BROKER).then(() => {
          return done() || null // promise warning fix
        }).catch(done)
      },

      // parse config trap error
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

      // prepare certs/keys needed by SDK plugin developer
      (done) => {
        let root = process.cwd()
        let home = path.join(root, 'keys')

        if (!fs.existsSync(home)) fs.mkdirSync(home)

        let writer = (content, filePath, cb) => {
          if (isEmpty(content)) return cb(null, '')
          fs.writeFile(filePath, content, (err) => { cb(err, filePath) })
        }

        async.parallel({
          ca: (cb) => { writer(process.env.CA, path.join(home, 'server-ca.pem'), cb) },
          crl: (cb) => { writer(process.env.CRL, path.join(home, 'server-crl.pem'), cb) },
          key: (cb) => { writer(process.env.KEY, path.join(home, 'server-key.pem'), cb) },
          cert: (cb) => { writer(process.env.CERT, path.join(home, 'server-cert.pem'), cb) }
        }, (err, res) => {
          Object.assign(this, res)

          process.env.CA = undefined
          process.env.CRL = undefined
          process.env.KEY = undefined
          process.env.CERT = undefined

          done(err)
        })
      },

      // setting up needed queues
      (done) => {
        let queueIds = [PLUGIN_ID, 'cmd.responses', 'devices', 'data', 'logs', 'exceptions']
          .concat(EXCEPTION_LOGGERS)
          .concat(COMMAND_RELAYS)
          .concat(OUTPUT_PIPES)
          .concat(LOGGERS)

        async.each(queueIds, (loggerId, cb) => {
          _broker.createQueue(loggerId).then(() => {
            return cb() || null // promise warning fix
          }).catch((err) => {
            if (!err) console.error('Gateway newQueue()', err)
            cb(err)
          })
        }, done)
      },

      // initialize topic exhange for messages
      (done) => {
        async.each(COMMAND_RELAYS, (cmdRelay, cb) => {
          _broker.createExchange(`${cmdRelay}.topic`).then((exchange) => {
            return exchange.consume((message) => {
              if (!isEmpty(message)) {
                async.waterfall([
                  async.constant(message.content.toString('utf8')),
                  async.asyncify(JSON.parse)
                ], (err, msg) => {
                  if (err) return console.error('Gateway exchange.consume() rcvd data is not a JSON.', err)

                  if (!_cmdCache.get(msg.sequenceId)) {
                    _cmdCache.set(msg.sequenceId, '0')

                    delete msg.sequenceId
                    this.emit('command', msg)
                  }
                })
              }
            })
          }).then(() => {
            return cb() || null // promise warning fix
          }).catch((err) => {
            if (!err) console.error('Gateway createExchange()', err)
            cb(err)
          })
        }, done)
      },

      // setting up RPC queue
      (done) => {
        _broker.createRPC('client', 'deviceinfo').then((queue) => {
          return queue.consume()
        }).then(() => {
          return done() || null
        }).catch(done)
      }

    ], (err) => {
      if (err) {
        console.error('Gateway: ', err)
        throw err
      }

      // plugin initialized
      process.nextTick(() => {
        this.emit('ready')
      })
    })

    this.pipe = (data, sequenceId) => {
      return new BPromise((resolve, reject) => {
        if (isEmpty(data) || !isPlainObject(data)) return reject(new Error('Invalid data received. Data should be and Object and should not empty.'))

        if (sequenceId) {
          _broker.queues['data'].publish({
            plugin: PLUGIN_ID,
            pipe: OUTPUT_PIPES,
            sequenceId: sequenceId,
            data: data
          }).then(() => {
            resolve()
          }).catch(reject)
        } else {
          async.each(OUTPUT_PIPES, (pipe, cb) => {
            _broker.queues[pipe].publish(data).then(() => {
              cb()
            }).catch(cb)
          }, (err) => {
            if (err) return reject(err)
            resolve()
          })
        }
      })
    }

    this.relayCommand = (command, devices, deviceGroups) => {
      return new BPromise((resolve, reject) => {
        if (!command) return reject(new Error('Kindly specify the command/message to send'))
        if (!isString(command)) return reject(new Error('Command must be a valid string'))
        if (isEmpty(devices) && isEmpty(deviceGroups)) return reject(new Error('Kindly specify the target device types or devices'))

        if (!(isString(deviceGroups) || Array.isArray(deviceGroups)) || !(isString(devices) || Array.isArray(devices))) {
          return reject(new Error(`'deviceGroups' and 'devices' must be a string or an array.`))
        }

        let sequenceId = uuid()

        async.each(COMMAND_RELAYS, (cmdRelay, cb) => {
          _broker.queues[cmdRelay].publish({
            command: command,
            sequenceId: sequenceId,
            deviceGroups: deviceGroups,
            devices: devices
          }).then(cb).catch(reject)
        }, (err) => {
          if (err) return reject(err)
          resolve()
        })
      })
    }

    this.sendCommandResponse = (commandId, response) => {
      return new BPromise((resolve, reject) => {
        if (!commandId || !isString(commandId)) return reject(new Error('Kindly specify the command id'))
        if (!response || !isString(response)) return reject(new Error('Kindly specify the response'))

        _broker.queues['cmd.responses'].publish({
          commandId: commandId,
          response: response
        }).then(() => {
          resolve()
        }).catch(reject)
      })
    }

    this.notifyConnection = (deviceId) => {
      return new BPromise((resolve, reject) => {
        if (!deviceId || !isString(deviceId)) return reject(new Error('Kindly specify a valid device identifier'))

        _broker.queues['devices'].publish({
          operation: 'connect',
          account: ACCOUNT,
          device: {
            _id: deviceId
          }
        }).then(() => {
          resolve()
        }).catch(reject)
      })
    }

    this.notifyDisconnection = (deviceId) => {
      return new BPromise((resolve, reject) => {
        if (!deviceId || !isString(deviceId)) return reject(new Error('Kindly specify a valid device identifier'))

        _broker.queues['devices'].publish({
          operation: 'disconnect',
          account: ACCOUNT,
          device: {
            _id: deviceId
          }
        }).then(() => {
          resolve()
        }).catch(reject)
      })
    }

    this.requestDeviceInfo = (deviceId) => {
      return new BPromise((resolve, reject) => {
        if (!deviceId || !isString(deviceId)) return reject(new Error('Kindly specify a valid device identifier'))

        let requestId = uuid()

        _broker.rpcs['deviceinfo'].once(requestId, (deviceInfo) => {
          resolve(deviceInfo)
        })

        _broker.rpcs['deviceinfo'].publish(requestId, {
          account: ACCOUNT,
          device: {
            _id: deviceId,
            requestId: requestId
          }
        }).then(() => {
          // Do nothing
        }).catch(reject)
      }).timeout(10000, 'Request for device information has timed out.')
    }

    this.syncDevice = (deviceInfo, deviceGroups) => {
      return new BPromise((resolve, reject) => {
        if (!deviceInfo || !isPlainObject(deviceInfo)) {
          return reject(new Error('Kindly specify a valid device information/details'))
        }

        if (!(hasProp(deviceInfo, '_id') || hasProp(deviceInfo, 'id'))) {
          return reject(new Error('Kindly specify a valid id for the device'))
        }

        if (!hasProp(deviceInfo, 'name')) {
          return reject(new Error('Kindly specify a valid name for the device'))
        }

        _broker.queues['devices'].publish({
          operation: 'sync',
          account: ACCOUNT,
          data: {
            group: deviceGroups,
            device: deviceInfo
          }
        }).then(() => {
          resolve()
        }).catch(reject)
      })
    }

    this.removeDevice = (deviceId) => {
      return new BPromise((resolve, reject) => {
        if (!deviceId || !isString(deviceId)) return reject(new Error('Kindly specify a valid device identifier'))

        _broker.queues['devices'].publish({
          operation: 'remove',
          account: ACCOUNT,
          device: {
            _id: deviceId
          }
        }).then(() => {
          resolve()
        }).catch(reject)
      })
    }

    this.setDeviceState = (deviceId, state) => {
      return new BPromise((resolve, reject) => {
        if (!deviceId || !isString(deviceId)) return reject(new Error('Kindly specify a valid device identifier'))
        if (isNil(state) || isEmpty(state)) return reject(new Error('Kindly specify the device state'))

        _broker.queues['devices'].publish({
          operation: 'setstate',
          account: ACCOUNT,
          device: {
            _id: deviceId,
            state: state
          }
        }).then(() => {
          resolve()
        }).catch(reject)
      })
    }

    this.log = (logData) => {
      return new BPromise((resolve, reject) => {
        if (isEmpty(logData)) return reject(new Error(`Please specify a data to log.`))
        if (!isPlainObject(logData) && !isString(logData)) return reject(new Error('Log data must be a string or object'))

        async.parallel([
          (callback) => {
            async.each(LOGGERS, (logger, done) => {
              _broker.queues[logger].publish(logData).then(() => {
                done()
              }).catch(done)
            }, callback)
          },
          (callback) => {
            _broker.queues['logs'].publish({
              account: ACCOUNT,
              data: logData
            }).then(() => {
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
                done()
              }).catch(done)
            }, callback)
          },
          (callback) => {
            _broker.queues['exceptions'].publish({
              account: ACCOUNT,
              data: errData
            }).then(() => {
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

module.exports = Gateway
