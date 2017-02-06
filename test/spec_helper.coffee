chai = require 'chai'
should = chai.should()
global.assert = chai.assert

global._ = require 'lodash'

req = require '../rethinkdb_esm'
global.RethinkDBESM = req.esm
rethinkdbdash = req.r

g = require('ger')
global.GER = g.GER
NamespaceDoestNotExist = GER.NamespaceDoestNotExist

RETHINKDB_HOST = process.env.RETHINKDB_HOST || '127.0.0.1'
RETHINKDB_PORT = process.env.RETHINKDB_PORT || 28015
RETHINKDB_DB = process.env.RETHINKDB_DB || 'hapiger_it'
RETHINKDB_TIMEOUT = process.env.RETHINKDB_TIMEOUT || 120000
RETHINKDB_BUFFER = process.env.RETHINKDB_BUFFER || 10
RETHINKDB_MAX = process.env.RETHINKDB_MAX || 50


r = rethinkdbdash({ host: RETHINKDB_HOST, port: RETHINKDB_PORT, db: RETHINKDB_DB, timeout: RETHINKDB_TIMEOUT, buffer: RETHINKDB_BUFFER, max: RETHINKDB_MAX})
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
  new RethinkDBESM({r: r}, NamespaceDoestNotExist)

#prepare databases
esm = new_esm()
bb.all([
  #TODO: find out where to place -> esm.try_delete_db(RETHINKDB_DB),
  esm.try_create_db(RETHINKDB_DB)
]).then(->

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

)