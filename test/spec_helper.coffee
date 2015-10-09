chai = require 'chai'
should = chai.should()
global.assert = chai.assert

global._ = require 'lodash'

req = require '../rethinkdb_esm'
global.RethinkDBESM = req.esm
rethinkdbdash = req.r

g = require('ger')
global.GER = g.GER

r = rethinkdbdash({ host: '127.0.0.1', port: 28015, db:'test', timeout: 120000, buffer:10 , max: 50})
global.bb = require 'bluebird'

global.moment = require "moment"


global.default_namespace = 'default'

global.last_week = moment().subtract(7, 'days')
global.three_days_ago = moment().subtract(2, 'days')
global.two_days_ago = moment().subtract(2, 'days')
global.yesterday = moment().subtract(1, 'days')
global.soon = moment().add(50, 'mins')
global.today = moment()
global.now = today
global.tomorrow = moment().add(1, 'days')
global.next_week = moment().add(7, 'days')


global.new_esm = (ESM) ->
  new ESM({r: r})

global.init_esm = (ESM, namespace = global.default_namespace) ->
  #in
  esm = new_esm(ESM)
  #drop the current tables, reinit the tables, return the esm
  bb.try(-> esm.destroy(namespace))
  .then( -> esm.initialize(namespace))
  .then( -> esm)

global.init_ger = (ESM, namespace = global.default_namespace) ->
  init_esm(ESM, namespace)
  .then( (esm) -> new GER(esm))

