bb = require 'bluebird'
fs = require 'fs'
crypto = require 'crypto'
moment = require 'moment'
shasum = null
_ = require 'lodash'
DEBUG = process.env.DEBUG || false

get_hash = (value) ->
  shasum = crypto.createHash("sha256")
  shasum.update(value.toString())
  shasum.digest("hex")

rethinkdbdash = require 'rethinkdbdash'

class RethinkDBESM

  ########################################
  ####    Initialization Functions    ####
  ########################################

  constructor: (orms = {}, @NamespaceDoestNotExist) ->
    @_r = orms.r
    @_DURABILITY = orms.durability || "soft"
    @_CONFLICT = orms.conflict || "update"
    @_BATCH_SIZE = orms.batch_size || 500
    @_READ_MODE = orms.read_mode || "outdated"

  try_create_db:(db) ->
    return @_r.dbCreate(db).run().then(-> true).catch(-> true)

  try_delete_db:(db) ->
    return @_r.dbDrop(db).run().then(-> true).catch(-> true)

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
          @_r.table("#{namespace}_events").indexCreate("expires_at",@_r.row("expires_at").default(false)).run(),

          @_r.table("#{namespace}_events").indexCreate("person").run(),
          @_r.table("#{namespace}_events").indexCreate("person_thing",[@_r.row("person"),@_r.row("thing")]).run(),

          @_r.table("#{namespace}_events").indexCreate("person_action",[@_r.row("person"),@_r.row("action")]).run(),
          @_r.table("#{namespace}_events").indexCreate("person_action_created_at",[@_r.row("person"),@_r.row("action"),@_r.row("created_at")]).run(),

          @_r.table("#{namespace}_events").indexCreate("person_created_at",[@_r.row("person"),@_r.row("created_at")]).run(),

          @_r.table("#{namespace}_events").indexCreate("person_action_expires_at",[@_r.row("person"),@_r.row("action"),@_r.row("expires_at").default(false)]).run(),
          @_r.table("#{namespace}_events").indexCreate("person_thing_created_at",[@_r.row("person"),@_r.row("thing"),@_r.row("created_at")]).run(),
          @_r.table("#{namespace}_events").indexCreate("person_expires_at_created_at",[@_r.row("person"),@_r.row("expires_at").default(false),@_r.row("created_at")]).run(),
          @_r.table("#{namespace}_events").indexCreate("person_action_expires_at_created_at",[@_r.row("person"),@_r.row("action"),@_r.row("expires_at").default(false),@_r.row("created_at")]).run(),
          @_r.table("#{namespace}_events").indexCreate("created_at_person_action_expires_at",[@_r.row("created_at"),@_r.row("person"),@_r.row("action"),@_r.row("expires_at").default(false)]).run(),

          @_r.table("#{namespace}_events").indexCreate("thing_action",[@_r.row("thing"),@_r.row("action")]).run(),
          @_r.table("#{namespace}_events").indexCreate("thing_action_person_created_at",[@_r.row("thing"),@_r.row("action"),@_r.row("person"),@_r.row("created_at")]).run(),
          @_r.table("#{namespace}_events").indexCreate("thing_action_created_at",[@_r.row("thing"),@_r.row("action"),@_r.row("created_at")]).run(),
          @_r.table("#{namespace}_events").indexCreate("thing_action_created_at_expires_at",[@_r.row("thing"),@_r.row("action"),@_r.row("created_at"),@_r.row("expires_at").default(false)]).run(),
          @_r.table("#{namespace}_events").indexCreate("thing_created_at",[@_r.row("thing"),@_r.row("created_at")]).run(),

          @_r.table("#{namespace}_events").indexCreate("action_created_at",[@_r.row("action"),@_r.row("created_at")]).run(),
          @_r.table("#{namespace}_events").indexCreate("action").run()

        ])
      bb.all(promises).then( => @_r.table("#{namespace}_events").indexWait().run())
    )
    .then( =>
      @_r.table("namespaces").insert({id: get_hash(namespace), namespace: namespace}, {conflict:  @_CONFLICT})
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
    batch = []
    namespace = events[0].namespace if events.length > 0

    for e in events
      created_at = @convert_date(e.created_at) || @_r.ISO8601(new Date().toISOString())
      expires_at =  @convert_date(e.expires_at)
      insert_attr = { namespace: e.namespace, person: e.person, action: e.action, thing: e.thing, created_at: created_at, expires_at: expires_at}
      insert_attr.id = get_hash(e.person.toString() + e.action + e.thing)
      batch.push(insert_attr)

      if batch.length > @_BATCH_SIZE
        promises.push(@add_events_batch(batch))
        batch = []

    if batch.length > 0
      promises.push(@add_events_batch(batch,namespace))

    bb.all(promises)

  add_events_batch: (events,namespace) ->
    return bb.try(->[]) if events.length == 0

    @_r.table("#{namespace}_events")
      .insert(events, {returnChanges: false, conflict:  @_CONFLICT, durability: @_DURABILITY})
      .run()
      .catch( (error) =>
        if error.message.indexOf("Table") > -1 and error.message.indexOf("does not exist") > -1
          throw new @NamespaceDoestNotExist()
    )

  add_event: (namespace, person, action, thing, dates = {}) ->
    created_at = @convert_date(dates.created_at) || @_r.ISO8601(new Date().toISOString())
    expires_at =  @convert_date(dates.expires_at)
    @add_event_to_db(namespace, person, action, thing, created_at, expires_at)

  add_event_to_db: (namespace, person, action, thing, created_at, expires_at = null) ->
    insert_attr = { person: person, action: action, thing: thing, created_at: created_at, expires_at: expires_at}
    insert_attr.id = get_hash(person.toString() + action + thing)

    @_r.table("#{namespace}_events")
    .insert(insert_attr, {returnChanges: false, conflict:  @_CONFLICT, durability: @_DURABILITY})
    .run()
    .catch( (error) =>
      if error.message.indexOf("Table") > -1 and error.message.indexOf("does not exist") > -1
        throw new @NamespaceDoestNotExist()
    )

  find_events: (namespace, options = {}) ->
    options = _.defaults(options,
      size: 50
      page: 0
      current_datetime: new Date()
    )
    r = @_r

    options.expires_after = moment(options.current_datetime).add(options.time_until_expiry, 'seconds').format() if options.time_until_expiry

    size = options.size
    page = options.page

    people = options.people

    person = options.person
    action = options.action
    thing = options.thing

    #TODO: implement find_events to accept actions and things #3
    if people
      q = @_r.table("#{namespace}_events")
      q = q.getAll(people..., {index: "person"} )
      q = q.orderBy(@_r.desc('created_at'))

      if (DEBUG)
        console.log("find_events query people -> ",q)

      q.run({readMode:@_READ_MODE})

    else if person and action and thing
      #Fast single look up
      @_event_selection(namespace, person, action, thing)
      .run({readMode:@_READ_MODE})
      .then( (e) ->
        return [] if !e
        [e] # have to put it into a list
      )
    else
      #optimized multi-event lookup
      index = null
      index_fields = null

      if person and action
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
        index_fields = [person]
      else if action and !person and !thing
        index = "action"
        index_fields = [action]
      else if thing and !action and !person
        index = "thing"
        index_fields = [thing]

      dt = r.minval
      if (options.expires_after)
        dt = @convert_date(options.expires_after)
        q = r.table("#{namespace}_events")
          .between([index_fields..., dt, r.minval], [index_fields..., r.maxval, @convert_date(options.current_datetime)],
        {index: index+'_expires_at_created_at'}).orderBy({index:r.desc(index+'_expires_at_created_at')})
          .slice(page*size, size*(page + 1))
      else
        q = r.table("#{namespace}_events")
          .between([index_fields...,r.minval], [index_fields...,@convert_date(options.current_datetime)],
          {index: index+'_created_at'}).orderBy({index:r.desc(index+'_created_at')})
          .slice(page*size, size*(page + 1))

      if (DEBUG)
        console.log("find_events query -> ",q)

      q.run({readMode:@_READ_MODE})

  delete_events: (namespace, options= {}) ->
    person = options.person
    action = options.action
    thing = options.thing
    @_event_selection(namespace, person, action, thing)
    .delete()
    .run({durability: @_DURABILITY})

  count_events: (namespace) ->
    @_r.table("#{namespace}_events").count().run({readMode:@_READ_MODE})

  estimate_event_count: (namespace) ->
    @_r.table("#{namespace}_events").count().run({readMode:@_READ_MODE})

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

    thing_actions = ({thing:thing, action:a} for a in actions)

    q = r.expr(thing_actions).concatMap((row) =>
          r.table("#{namespace}_events").between([row("thing"),row("action"),r.minval, @convert_date(options.expires_after)], [row("thing"),row("action"), @convert_date(options.current_datetime),r.maxval],
          {index: 'thing_action_created_at_expires_at'})
          .orderBy({index:r.desc("thing_action_created_at_expires_at")})
        )
    q = q.limit(options.neighbourhood_search_size)
    .concatMap((row) =>
      r.expr(thing_actions).concatMap((row1) =>
        r.table("#{namespace}_events").getAll([row("person"), row1('action')],{index: "person_action"})
          .filter((row) -> row("thing").ne(thing))
      )
    )
    .group("thing")
    .ungroup()
    .map((row) =>
        {
          thing: row("group"),
          people: row("reduction").map((row) -> row('person')).distinct()
          last_actioned_at: row("reduction").map((row) -> row('created_at')).max()
          last_expires_at: row("reduction").map((row) -> row('expires_at')).max()
          count: row("reduction").count()
        }
    )
    .filter( (row) =>
      row('last_expires_at').ge(@convert_date(options.expires_after))
    )
    .orderBy(r.desc("count"))
    .limit(options.neighbourhood_size)

    if (DEBUG)
      console.log("thing_neighbourhood query -> ",q)

    q.run({readMode:@_READ_MODE})
    # .then( (ret) ->
    #   console.log JSON.stringify(ret,null,2)
    #   ret
    # )


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
      value_actions.push {"#{column1}": value, "weight":weight, "action":a}
      for v in values
        value_actions.push {"#{column1}": v, "weight":weight, "action":a}

    q  = r.expr(value_actions)
    .concatMap((row) =>
      r.table("#{namespace}_events").between([row(column1),row("action"), r.minval],[row(column1),row("action"), @convert_date(now)], {index: "#{column1}_action_created_at"})
      .orderBy({index:r.desc("#{column1}_action_created_at")})
      .limit(limit)
      .innerJoin(r.expr(action_weights), (row, actions) -> row('action').eq(actions('action')))
      .zip()
      .merge((row) => {days_since: row('created_at').sub(@convert_date(now)).div(86400).round()})
      .merge(r.js("( function(row) { return { weight:  row.weight * Math.pow(#{event_decay_rate}, - Math.abs(row.days_since)) } } )"))
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

    if (DEBUG)
      console.log("get_cosine_distances query -> ",q)

    q.run({readMode:@_READ_MODE})
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
    # .then( (ret) ->
    #   console.log JSON.stringify(ret,null,2)
    #   ret
    # )

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
    q  = r.table("#{namespace}_events")

    person_actions = ({person:person, action:a} for a in actions)
    q = r.expr(person_actions).concatMap((row) =>
          r.table("#{namespace}_events").between([row('person'),row('action'), r.minval], [row('person'), row('action'), @convert_date(options.current_datetime)],
            {index: 'person_action_created_at',leftBound:'open',rightBound: 'open'}).orderBy({index:r.desc("person_action_created_at")})
    )

    q = q.limit(options.neighbourhood_search_size)

    q = q.concatMap((row) =>
      r.expr(person_actions).concatMap((row1) =>
        r.table("#{namespace}_events")
          .between([row("thing"),row1("action"),r.minval,r.minval],
          [row("thing"), row1("action"),r.maxval, @convert_date(options.current_datetime)],
          {index: 'thing_action_person_created_at'})
          .filter((row) -> row("person").ne(person))
        )
    )

    q = q.group("person")
    .ungroup()
    .map((row) =>
      {
        person: row("group"),
        count: row("reduction").count()
      }
    )

    q = q.filter( (row) =>
      r.expr(person_actions).concatMap((row1) =>
        r.table("#{namespace}_events")
          .between([row("person"),row1("action"), @convert_date(expires_after)],
          [row("person"), row1("action"), r.maxval],
          {index: 'person_action_expires_at'})
        ).group("person").ungroup().count().gt(0)
      )

    q = q.orderBy(r.desc("count"))
    .limit(options.neighbourhood_size)("person")

    if (DEBUG)
      console.log("person_neighbourhood query -> ",q)

    q.run({readMode:@_READ_MODE})

  calculate_similarities_from_person: (namespace, person, people, actions, options={}) ->
    @_similarities(namespace, 'person', 'thing', person, people, actions, options)



  filter_things_by_previous_actions: (namespace, person, things, actions) ->
    return bb.try(-> things) if !actions or actions.length == 0 or things.length == 0
    indexes = []
    indexes.push([person, action]) for action in actions
    q = @_r(things).setDifference(@_r.table("#{namespace}_events").getAll(@_r.args(indexes),{index: "person_action"})
    .coerceTo("ARRAY")("thing"))
    if (DEBUG)
      console.log("filter_things_by_previous_actions query -> ",q)
    q.run({readMode:@_READ_MODE})

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
        for a in actions
          people_actions.push {person: p, action: a}

    q = r.expr(people_actions)
      .concatMap((row) =>
        r.table("#{namespace}_events")
        .between([row('person'), row('action'), @convert_date(expires_after), r.minval], [row('person'), row('action'), r.maxval, @convert_date(options.current_datetime)],
        {index: 'person_action_expires_at_created_at',leftBound:'closed',rightBound: 'closed'})
        #TODO: remove this filter finding why between for current_datetime fails
        .filter( (row) => row('created_at').le(@convert_date(options.current_datetime)))
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

    if (DEBUG)
      console.log("recent_recommendations_by_people query -> ",q)

    q.run({readMode:@_READ_MODE})

  ############################################
  #### END Person Recommendation Function ####
  ############################################



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
    q = @_r.table("#{namespace}_events", )
    #.sample(10000)
    .group('thing')
    .count()
    .ungroup()
    .orderBy(@_r.desc('reduction'))
    .limit(100)('group')

    if (DEBUG)
      console.log("get_active_things query -> ",q)

    q.run({readMode:@_READ_MODE})

  get_active_people: (namespace) ->
    #Select 10K events, count frequencies order them and return
    q = @_r.table("#{namespace}_events", )
    #.sample(10000)
    .group('person')
    .count()
    .ungroup()
    .orderBy(@_r.desc('reduction'))
    .limit(100)('group')

    if (DEBUG)
      console.log("get_active_people query -> ",q)

    q.run({readMode:@_READ_MODE})

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
        promises.push @_r.table("#{namespace}_events").between([thing, action,@_r.minval],[thing, action,@_r.maxval],{index: "thing_action_created_at"}).orderBy(@_r.desc("thing_action_created_at")).skip(trunc_size).delete().run({ durability: @_DURABILITY})
    #cut each action down to size
    bb.all(promises)


  truncate_people_per_action: (namespace, people, trunc_size, actions) ->
    #TODO do the same thing for things
    return bb.try( -> []) if people.length == 0
    promises = []
    for person in people
      for action in actions
        promises.push @_r.table("#{namespace}_events").between([person, action,@_r.minval],[person, action,@_r.maxval],{index: "person_action_created_at"}).orderBy({index:@_r.desc("person_action_created_at")}).skip(trunc_size).delete().run({durability: @_DURABILITY})
    #cut each action down to size
    bb.all(promises)

  remove_events_till_size: (namespace, number_of_events) ->
    #TODO move too offset method
    #removes old events till there is only number_of_events left
    @_r.table("#{namespace}_events").orderBy({index: @_r.desc("created_at")})
    .skip(number_of_events).delete().run({durability: @_DURABILITY})

  ###########################################
  ####     END Compact Function          ####
  ###########################################

module.exports = {
  esm: RethinkDBESM
  r: rethinkdbdash
}
