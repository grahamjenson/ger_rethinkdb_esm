bb = require 'bluebird'
fs = require 'fs'
crypto = require 'crypto'
moment = require 'moment'
shasum = null
_ = require 'lodash'

g = require 'ger'

Errors = g.Errors

get_hash = (value) ->
  shasum = crypto.createHash("sha256")
  shasum.update(value.toString())
  shasum.digest("hex")

class RethinkDBESM

  ########################################
  ####    Initialization Functions    ####
  ########################################

  constructor: (orms) ->
    @_r = orms.r

  try_create_table: (table, table_list) ->
    if table in table_list
      return bb.try(-> false)
    else
      return @_r.tableCreate(table).run().then(-> true)

  try_delete_table: (table, table_list) ->
    if table in table_list
      #DELETES all the events because drop and create are VERY slow
      return @_r.table(table).delete().run().then(( ret ) -> true)
    else
      return bb.try(-> false)

  try_delete_namespace: (namespace, table_list) ->
    if 'namespaces' in table_list
      @_r.table('namespaces')
      .filter({namespace: namespace})
      .delete().run()
      .then(( ret ) -> true)
    else
      return bb.try(-> false)

  destroy: (namespace) ->
    @_r.tableList().run().then( (list) =>
      bb.all([
        @try_delete_table("#{namespace}_events", list),
        @try_delete_namespace(namespace, list) #table is only a marker
      ])
    )

  exists: (namespace) ->
    @list_namespaces().then( (list) =>
      return namespace in list
    )

  list_namespaces: () ->
    @_r.table('namespaces').run()
    .then( (ret) ->
      ret.map((r) -> r.namespace)
    )

  initialize: (namespace) ->
    @_r.tableList().run().then( (list) =>
      bb.all([
        @try_create_table("#{namespace}_events", list)
        @try_create_table("namespaces", list)
      ])
    )
    .spread( (events_created, schema_created) =>
      promises = []
      if events_created
        promises = promises.concat([
          @_r.table("#{namespace}_events").indexCreate("created_at").run(),
          @_r.table("#{namespace}_events").indexCreate("expires_at").run(),
          @_r.table("#{namespace}_events").indexCreate("person").run(),
          @_r.table("#{namespace}_events").indexCreate("person_thing",[@_r.row("person"),@_r.row("thing")]).run(),
          @_r.table("#{namespace}_events").indexCreate("thing_action",[@_r.row("thing"),@_r.row("action")]).run(),
          @_r.table("#{namespace}_events").indexCreate("person_action",[@_r.row("person"),@_r.row("action")]).run(),
          @_r.table("#{namespace}_events").indexCreate("person_action_created_at",[@_r.row("person"),@_r.row("action"),@_r.row("created_at")]).run(),
          @_r.table("#{namespace}_events").indexCreate("thing").run(),
          @_r.table("#{namespace}_events").indexCreate("action").run(),
          @_r.table("#{namespace}_events").indexWait().run()
        ])
      bb.all(promises)
    )
    .then( =>
      @_r.table("namespaces").insert({id: get_hash(namespace), namespace: namespace}, {conflict:  "update"})
    )

  ########################################
  #### END Initialization Functions   ####
  ########################################



  ########################################
  ####        Events Functions        ####
  ########################################


  convert_date: (date) ->
    if date
      date = new Date(date)
      if date._isAMomentObject
        date = date.format()
      valid_date = moment(date, moment.ISO_8601);
      if(valid_date.isValid())
        ret = @_r.ISO8601(date)
      else
        ret = @_r.ISO8601(date.toISOString())
      return ret
    else
      return null

  add_events: (events) ->
    promises = []
    for e in events
      promises.push @add_event(e.namespace, e.person, e.action, e.thing, {created_at: e.created_at, expires_at: e.expires_at})
    bb.all(promises)

  add_event: (namespace, person, action, thing, dates = {}) ->
    created_at = @convert_date(dates.created_at) || @_r.ISO8601(new Date().toISOString())
    expires_at =  @convert_date(dates.expires_at)
    @add_event_to_db(namespace, person, action, thing, created_at, expires_at)

  add_event_to_db: (namespace, person, action, thing, created_at, expires_at = null) ->
    insert_attr = { person: person, action: action, thing: thing, created_at: created_at, expires_at: expires_at}
    insert_attr.id = get_hash(person.toString() + action + thing)

    @_r.table("#{namespace}_events")
    .insert(insert_attr, {conflict:  "update", durability: "soft"})
    .run()
    .catch( (error) ->
      if error.message.indexOf("Table") > -1 and error.message.indexOf("does not exist") > -1
        throw new Errors.NamespaceDoestNotExist()
    )

  find_events: (namespace, options = {}) ->
    options = _.defaults(options,
      size: 50
      page: 0
      current_datetime: new Date()
    )

    options.expires_after = moment(options.current_datetime).add(options.time_until_expiry, 'seconds').format() if options.time_until_expiry

    size = options.size
    page = options.page

    person = options.person
    action = options.action
    thing = options.thing

    if person and action and thing
      #Fast single look up
      @_event_selection(namespace, person, action, thing)
      .run()
      .then( (e) ->
        return [] if !e
        [e] # have to put it into a list
      )
    else
      #slower multi-event lookup
      @_event_selection(namespace, person, action, thing)
      .filter((row) => row('expires_at').ge(@convert_date(options.expires_after)))
      .filter((row) => row('created_at').le(@convert_date(options.current_datetime)))
      .slice(page*size, size*(page + 1))
      .run()

  delete_events: (namespace, options= {}) ->
    person = options.person
    action = options.action
    thing = options.thing
    @_event_selection(namespace, person, action, thing)
    .delete()
    .run({durability: "soft"})

  count_events: (namespace) ->
    @_r.table("#{namespace}_events").count().run()

  estimate_event_count: (namespace) ->
    @_r.table("#{namespace}_events").count().run()

  _event_selection: (namespace, person, action, thing) ->
    single_selection = false
    index = null
    index_fields = null
    if person and action and thing
      single_selection = true
    else if person and action
      index = "person_action"
      index_fields = [person, action]
    else if person and thing
      index = "person_thing"
      index_fields = [person, thing]
    else if action and thing
      index = "thing_action"
      index_fields = [thing, action]
    else if person and !action and !thing
      index = "person"
      index_fields = person
    else if action and !person and !thing
      index = "action"
      index_fields = action
    else if thing and !action and !person
      index = "thing"
      index_fields = thing

    q = @_r.table("#{namespace}_events")
    if single_selection
      q = q.get(get_hash(person.toString() + action + thing))
    else if index
      q = q.getAll(index_fields,{index: index})
      q = q.orderBy(@_r.desc('created_at'))
    return q


  ########################################
  ####    END Events Functions        ####
  ########################################



  ###########################################
  ####  Thing Recommendation Function    ####
  ###########################################


  thing_neighbourhood: (namespace, thing, actions, options = {}) ->
    return bb.try(-> []) if !actions or actions.length == 0

    options = _.defaults(options,
      neighbourhood_size: 100
      neighbourhood_search_size: 500
      time_until_expiry: 0
      current_datetime: new Date()
    )
    options.expires_after = moment(options.current_datetime).add(options.time_until_expiry, 'seconds').format()

    r = @_r
    thing_actions = ([thing, a] for a in actions)
    r.table("#{namespace}_events")
    .getAll(thing_actions..., {index: "thing_action"} )
    .filter( (row) =>
      row('created_at').le(@convert_date(options.current_datetime))
    )
    .orderBy(r.desc('created_at'))
    .limit(options.neighbourhood_search_size)
    .concatMap((row) =>
      r.table("#{namespace}_events").getAll([row("person"),row("action")],{index: "person_action"})
      .filter((row) ->
        row("thing").ne(thing)
      )
    )
    .group("thing")
    .ungroup()
    .map((row) =>
      {
        thing: row("group"),
        people: row("reduction").map((row) -> row('person')).distinct()
        last_actioned_at: row("reduction").map( (row) -> row('created_at')).max()
        last_expires_at: row("reduction").map( (row) -> row('expires_at')).max()
        count: row("reduction").count()
      }
    )
    .filter( (row) =>
      row('last_expires_at').ge(@convert_date(options.expires_after))
    )
    .orderBy(r.desc("count"))
    .limit(options.neighbourhood_size)
    .run()



  cosine_distance: (p1_values, p2_values) ->
    return 0 if !p1_values || !p2_values

    numerator = 0
    for value, weight of p1_values
      if p2_values[value]
        numerator += weight * p2_values[value]

    denominator_1 = 0
    for value, weight of p1_values
      denominator_1 += Math.pow(weight,2)

    denominator_2 = 0
    for value, weight of p2_values
      denominator_2 += Math.pow(weight,2)

    numerator/(Math.sqrt(denominator_1)*Math.sqrt(denominator_2))

  get_cosine_distances: (namespace, column1, column2, value, values, actions, limit, event_decay_rate, now) ->
    return bb.try(->[]) if values.length == 0
    bindings = {value: value, now: now, event_decay_rate: event_decay_rate}
    r = @_r

    action_weights = []
    value_actions = []
    for a, weight of actions
      action_weights.push {action:a, weight: weight}
      value_actions.push {"#{column1}": value}
      for v in values
        value_actions.push {"#{column1}": v}


    r.expr(value_actions)
    .concatMap((row) =>
      r.table("#{namespace}_events")
      .getAll(row(column1), {index: "#{column1}"})
      .filter( (row) => row('created_at').le(@convert_date(now)))
      .filter((row) -> r.expr(Object.keys(actions)).contains(row('action')))
      .orderBy(r.desc("created_at"))
      .limit(limit)
      .innerJoin(r.expr(action_weights), (row, actions) -> row('action').eq(actions('action')))
      .zip()
      .merge((row) => {days_since: row('created_at').sub(@convert_date(now)).div(86400).round()})
      .merge(r.js("( function(row) { return { weight:  row.weight * Math.pow(#{event_decay_rate}, - row.days_since) } } )"))
      .group("person", "thing")
      .max('weight')
      .ungroup()
    )
    .map((row) -> row('reduction'))
    .group(column1)
    .map((row) ->
      {
        value: row(column2)
        weight: row("weight")
      }
    )
    .ungroup()
    .run()
    .then( (ret) =>
      value_actions = {}
      for g in ret
        value_actions[g.group] = {}
        for red in g.reduction
          value_actions[g.group][red.value] = red.weight

      value_diffs = {}
      for v in values
        value_diffs[v] = @cosine_distance(value_actions[value], value_actions[v]) || 0
      value_diffs
    )
    .then( (ret) ->
      ret
    )

  _similarities: (namespace, column1, column2, value, values, actions, options={}) ->
    return bb.try(-> {}) if !actions or actions.length == 0 or values.length == 0
    options = _.defaults(options,
      similarity_search_size: 500
      event_decay_rate: 1
      current_datetime: new Date()
    )
    #TODO history search size should be more [similarity history search size]
    @get_cosine_distances(namespace, column1, column2, value, values, actions, options.similarity_search_size, options.event_decay_rate, options.current_datetime)

  calculate_similarities_from_thing: (namespace, thing, things, actions, options={}) ->
    @_similarities(namespace, 'thing', 'person', thing, things, actions, options)


  ###########################################
  #### End Thing Recommendation Function ####
  ###########################################

  ###########################################
  #### Person Recommendation Function    ####
  ###########################################

  person_neighbourhood: (namespace, person, actions, options = {}) ->
    return bb.try(-> []) if !actions or actions.length == 0

    options = _.defaults(options,
      neighbourhood_size: 100
      neighbourhood_search_size: 500
      time_until_expiry: 0
      current_datetime: new Date()
    )
    expires_after = moment(options.current_datetime).add(options.time_until_expiry, 'seconds').format()

    r = @_r
    person_actions = ([person, a] for a in actions)

    r.table("#{namespace}_events")
    .getAll(person_actions..., {index: "person_action"} )
    .filter( (row) => row('created_at').le(@convert_date(options.current_datetime)))
    .orderBy(r.desc('created_at'))
    .limit(options.neighbourhood_search_size)
    .concatMap((row) =>
      r.table("#{namespace}_events")
      .getAll(row("thing"),{index: "thing"})
      .filter((row) -> r.expr(actions).contains(row('action')))
      .filter((row) -> row("person").ne(person))
      .filter( (row) => row('created_at').le(@convert_date(options.current_datetime)))
    )
    .group("person")
    .ungroup()
    .map((row) =>
      {
        person: row("group"),
        count: row("reduction").count()
      }
    )
    .filter( (row) =>
      r.table("#{namespace}_events")
      .getAll(row("person"),{index: "person"})
      .filter((row) -> r.expr(actions).contains(row('action')))
      .filter((row) => row('expires_at').ge(@convert_date(expires_after)))
      .count()
      .gt(0)
    )
    .orderBy(r.desc("count"))
    .limit(options.neighbourhood_size)("person")
    .run()

  calculate_similarities_from_person: (namespace, person, people, actions, options={}) ->
    @_similarities(namespace, 'person', 'thing', person, people, actions, options)



  filter_things_by_previous_actions: (namespace, person, things, actions) ->
    return bb.try(-> things) if !actions or actions.length == 0 or things.length == 0
    indexes = []
    indexes.push([person, action]) for action in actions
    @_r(things).setDifference(@_r.table("#{namespace}_events").getAll(@_r.args(indexes),{index: "person_action"})
    .coerceTo("ARRAY")("thing")).run()

  recent_recommendations_by_people: (namespace, actions, people, options = {}) ->
    return bb.try(->[]) if people.length == 0 || actions.length == 0

    options = _.defaults(options,
      recommendations_per_neighbour: 10
      time_until_expiry: 0
      current_datetime: new Date()
    )
    expires_after = moment(options.current_datetime).add(options.time_until_expiry, 'seconds').format()


    r = @_r
    people_actions = []
    for p in people
      people_actions.push {person: p}

    r.expr(people_actions)
    .concatMap((row) =>
      r.table("#{namespace}_events")
      .getAll(row('person'), {index: "person"})
      .filter( (row) => row('created_at').le(@convert_date(options.current_datetime)))
      .filter( (row) => row('expires_at').ge(@convert_date(expires_after)))
      .filter((row) -> r.expr(actions).contains(row('action')))
      .orderBy(r.desc("created_at"))
      .limit(options.recommendations_per_neighbour)
    )
    .group('person', 'thing')
    .ungroup()
    .map( (row) ->
      {
        thing: row("group").nth(1)
        person: row("group").nth(0)
        last_actioned_at: row("reduction").map( (row) -> row('created_at')).max()
        last_expires_at: row("reduction").map( (row) -> row('expires_at')).max()
      }
    )
    .orderBy(r.desc("last_actioned_at"))
    .run()

  ###########################################
  #### END Person Recommendation Function####
  ###########################################



  ###########################################
  ####         Compact Function          ####
  ###########################################

  pre_compact: ->
    bb.try -> true

  post_compact: ->
    bb.try -> true

  remove_non_unique_events_for_people: (people) ->
    bb.try( -> [])

  remove_non_unique_events_for_person: (people) ->
    bb.try( -> [])

  get_active_things: (namespace) ->
    #Select 10K events, count frequencies order them and return
    @_r.table("#{namespace}_events", ).sample(10000)
    .group('thing')
    .count()
    .ungroup()
    .orderBy(@_r.desc('reduction'))
    .limit(100)('group').run()

  get_active_people: (namespace) ->
    #Select 10K events, count frequencies order them and return
    @_r.table("#{namespace}_events", ).sample(10000)
    .group('person')
    .count()
    .ungroup()
    .orderBy(@_r.desc('reduction'))
    .limit(100)('group').run()

  compact_people : (namespace, compact_database_person_action_limit, actions) ->
    @get_active_people(namespace)
    .then( (people) =>
      @truncate_people_per_action(namespace, people, compact_database_person_action_limit, actions)
    )

  compact_things :  (namespace, compact_database_thing_action_limit, actions) ->
    @get_active_things(namespace)
    .then( (things) =>
      @truncate_things_per_action(namespace, things, compact_database_thing_action_limit, actions)
    )

  truncate_things_per_action: (namespace, things, trunc_size, actions) ->

    #TODO do the same thing for things
    return bb.try( -> []) if things.length == 0
    #cut each action down to size
    promises = []
    for thing in things
      for action in actions
        promises.push @_r.table("#{namespace}_events").getAll([thing, action], {index: "thing_action"}).orderBy(@_r.desc("created_at")).skip(trunc_size).delete().run({ durability: "soft"})

    bb.all(promises)


  truncate_people_per_action: (namespace, people, trunc_size, actions) ->
    #TODO do the same thing for things
    return bb.try( -> []) if people.length == 0
    promises = []
    for person in people
      for action in actions
        promises.push @_r.table("#{namespace}_events").getAll([person, action],{index: "person_action"}).orderBy(@_r.desc("created_at")).skip(trunc_size).delete().run({durability: "soft"})
    #cut each action down to size
    bb.all(promises)

  remove_events_till_size: (namespace, number_of_events) ->
    #TODO move too offset method
    #removes old events till there is only number_of_events left
    @_r.table("#{namespace}_events").orderBy({index: @_r.desc("created_at")})
    .skip(number_of_events).delete().run({durability: "soft"})

  ###########################################
  ####     END Compact Function          ####
  ###########################################

module.exports = RethinkDBESM;
