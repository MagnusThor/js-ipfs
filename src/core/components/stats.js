'use strict'

const promisify = require('promisify-es6')
const Big = require('big.js')
const Pushable = require('pull-pushable')
const human = require('human-to-milliseconds')
const toStream = require('pull-stream-to-stream')

function bandwidthStats (self, opts) {
  // TODO: get the true stats :)
  return new Promise((resolve, reject) => {
    resolve({
      totalIn: new Big(0),
      totalOut: new Big(0),
      rateIn: new Big(0),
      rateOut: new Big(0)
    })
  })
}

module.exports = function stats (self) {
  const _bwPullStream = (opts) => {
    let interval = null
    let stream = Pushable(true, () => {
      if (interval) {
        clearInterval(interval)
      }
    })

    if (opts.poll) {
      human(opts.interval || '1s', (err, value) => {
        if (err) throw err

        interval = setInterval(() => {
          bandwidthStats(self, opts)
            .then((stats) => stream.push(stats))
            .catch((err) => stream.end(err))
        }, value)
      })
    } else {
      bandwidthStats(self, opts)
        .then((stats) => {
          stream.push(stats)
          stream.end()
        })
        .catch((err) => stream.end(err))
    }

    return stream.source
  }

  return {
    bitswap: require('./bitswap')(self).stat,
    repo: require('./repo')(self).stat,
    bw: promisify((opts, callback) => {
      bandwidthStats(self, opts)
        .then((stats) => callback(null, stats))
        .catch((err) => callback(err))
    }),
    bwReadableStream: (opts) => toStream.source(_bwPullStream(opts)),
    bwPullStream: _bwPullStream
  }
}
