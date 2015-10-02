bb = require 'bluebird'
fs = require 'fs'
crypto = require 'crypto'
moment = require 'moment'
shasum = null
_ = require 'lodash'


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

  try_drop_table: (table, table_list) ->
    if table in table_list
      # drop and create are VERY slow
      return @_r.tableDrop(table).run().then(( ret ) -> true)
    else
      return bb.try(-> false)

  destroy: (namespace) ->
    @_r.tableList().run().then( (list) =>
      bb.all([
        @try_delete_table("#{namespace}_events", list),
        @try_drop_table("#{namespace}_schema", list) #table is only a marker
      ])
    )

  exists: (namespace) ->
    @list_namespaces().then( (list) =>
      return namespace in list
    )

  list_namespaces: () ->
    @_r.tableList().run().then( (list) =>
      list = (li.replace('_schema','') for li in list when _.contains(li, '_schema'))
    )

  initialize: (namespace) ->
    @_r.tableList().run().then( (list) =>
      bb.all([
        @try_create_table("#{namespace}_events", list)
        @try_create_table("#{namespace}_schema", list)
      ])
    )
    .spread( (events_created, schema_created) =>
      promises = []
      if events_created
        promises = promises.concat([@_r.table("#{namespace}_events").indexCreate("created_at").run(),
          @_r.table("#{namespace}_events").indexCreate("expires_at").run(),
          @_r.table("#{namespace}_events").indexCreate("person").run(),
          @_r.table("#{namespace}_events").indexCreate("person_thing",[@_r.row("person"),@_r.row("thing")]).run(),
          @_r.table("#{namespace}_events").indexCreate("action_thing",[@_r.row("action"),@_r.row("thing")]).run(),
          @_r.table("#{namespace}_events").indexCreate("person_action",[@_r.row("person"),@_r.row("action")]).run(),
          @_r.table("#{namespace}_events").indexCreate("person_action_created_at",[@_r.row("person"),@_r.row("action"),@_r.row("created_at")]).run(),
          @_r.table("#{namespace}_events").indexCreate("thing").run(),
          @_r.table("#{namespace}_events").indexCreate("action").run(),
          @_r.table("#{namespace}_events").indexWait().run()
        ])
      bb.all(promises)
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

    @_r.table("#{namespace}_events").insert(insert_attr, {conflict:  "update", durability: "soft"}).run()
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
      .slice(page*size, size*(page + 1))
      .run()

  delete_events: (namespace, person, action, thing) ->
    @_event_selection(namespace, person, action, thing)
    .delete()
    .run({useOutdated: true,durability: "soft"})


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
      index = "action_thing"
      index_fields = [action, thing]
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
    action_things = ([a, thing] for a in actions)
    r.table("#{namespace}_events")
    .getAll(action_things..., {index: "action_thing"} )
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
    .then( (ret) ->
      ret
    )
    #

    # one_degree_away = @_one_degree_away(namespace, 'thing', 'person', thing, actions, options)
    # .orderByRaw("action_count DESC")

    # @_knex(one_degree_away.as('x'))
    # .where('x.last_expires_at', '>', options.expires_after)
    # .where('x.last_actioned_at', '<=', options.current_datetime)
    # .orderByRaw("x.action_count DESC")
    # .limit(options.neighbourhood_size)
    # .then( (rows) ->
    #   for row in rows
    #     row.people = _.uniq(row.person) # difficult in postgres
    #   rows
    # )

  calculate_similarities_from_thing: () ->


  ###########################################
  #### End Thing Recommendation Function ####
  ###########################################

  ###########################################
  #### Person Recommendation Function    ####
  ###########################################

  person_neighbourhood: (namespace, person, actions, action, limit = 100, search_limit = 500) ->
    return bb.try(-> []) if !actions or actions.length == 0
    r = @_r
    person_actions = ([person, a] for a in actions)
    r.table("#{namespace}_events").getAll(person_actions..., {index: "person_action"} )
    .orderBy(r.desc('created_at'))
    .limit(search_limit)
    .concatMap((row) =>
      r.table("#{namespace}_events").getAll([row("action"),row("thing")],{index: "action_thing"})
      .filter((row) ->
        row("person").ne(person)
      )
      .map((row) ->
        {person: row("person"),action: row("action")}
      )
    )
    .group("person")
    .ungroup()
    .filter((row) =>
      row("reduction")("action").contains(action)
    )
    .map((row) =>
      {
        person: row("group"),
        count: row("reduction").count()
      }
    )
    .orderBy(r.desc('count'))
    .limit(limit)("person")
    .run()


  filter_things_by_previous_actions: (namespace, person, things, actions) ->
    return bb.try(-> things) if !actions or actions.length == 0 or things.length == 0
    indexes = []
    indexes.push([person, action]) for action in actions
    @_r(things).setDifference(@_r.table("#{namespace}_events").getAll(@_r.args(indexes),{index: "person_action"})
    .coerceTo("ARRAY")("thing")).run()

  recent_recommendations_by_people: (namespace, action, people, limit = 50, expires_after = new Date().toISOString()) ->
    return bb.try(->[]) if people.length == 0
    r = @_r
    people_actions = []
    for p in people
      people_actions.push [p,action]

    r.table("#{namespace}_events")
    .getAll(r.args(people_actions), {index: 'person_action'})
    .group("person","action")
    .orderBy(r.desc("created_at"))
    .limit(limit)
    .map((row) ->
      {
        thing: row("thing"),
        last_actioned_at: row("created_at").toEpochTime()
      }
    )
    .ungroup()
    .map((row) =>
      r.object(row("group").nth(0).coerceTo("string"),row("reduction"))
    )
    .reduce((a,b) ->
      a.merge(b)
    )
    .default({})
    .run()


  calculate_similarities_from_person: (namespace, person, people, actions, person_history_limit, recent_event_days) ->
    @get_jaccard_distances_between_values(namespace, person, people, actions, person_history_limit, recent_event_days)


  get_jaccard_distances_between_values: (namespace, person, people, actions, limit = 500, days_ago=14) ->
    return bb.try(->[]) if people.length == 0
    r = @_r

    people_actions = []

    for a in actions
      people_actions.push [person, a]
      for p in people
        people_actions.push [p,a]

    r.table("#{namespace}_events")
    .getAll(r.args(people_actions), {index: 'person_action'})
    .group("person","action")
    .orderBy(r.desc("created_at"))
    .limit(limit).ungroup()
    .map((row) ->
      r.object(
        row("group").nth(0).coerceTo("string"),
        r.object(
          row("group").nth(1).coerceTo("string"),
          {
            history: row("reduction")("thing"),
            recent_history: row("reduction").filter((row) =>
              row("created_at").during(r.now().sub(days_ago * 24 * 60 * 60),r.now())
            )("thing")
          }
        )
      )
    ).reduce((a,b) ->
        a.merge(b)
    ).do((rows) ->
      r(actions).concatMap((a) ->
        r(a).coerceTo("string").do (_a) ->
          rows(r(person).coerceTo("string"))(_a).default(null).do (person_histories) ->
            r.branch(person_histories.ne(null), person_histories("history"),[]).coerceTo("array").do (person_history) ->
              r.branch(person_histories.ne(null), person_histories("recent_history"),[]).coerceTo("array").do (person_recent_history) ->
                r(people).map((p) ->
                  r(p).coerceTo("string").do (_p) ->
                    rows(_p)(_a).default(null).do (p_histories) ->
                      r.branch(p_histories.ne(null),p_histories("history"),[]).coerceTo("array").do (p_history) ->
                        r.branch(p_histories.ne(null),p_histories("recent_history"),[]).coerceTo("array").do (p_recent_history) ->
                          r.object(_p,r.object(_a,r(p_history).setIntersection(person_history).count().div(r([r(p_history).setUnion(person_history).count(),1]).max()))).do (limit_distance) ->
                            r.object(_p,r.object(_a,r(p_recent_history).setIntersection(person_recent_history).count().div(r([r.expr(p_recent_history).setUnion(person_recent_history).count(),1]).max()))).do (recent_distance) ->
                              {
                                  limit_distance: limit_distance,
                                  recent_distance: recent_distance
                              }
                )
        )
      .reduce((a,b) ->
        {
          limit_distance: a("limit_distance").merge(b("limit_distance")),
          recent_distance: a("recent_distance").merge(b("recent_distance"))
        }
      )
    )
    .do((data) ->
      r(people).concatMap((p) ->
        r(p).coerceTo("string").do (_p) ->
          r(actions).map((a) ->
            r(a).coerceTo("string").do (_a) ->
              r.branch(data("recent_distance")(_p).ne(null).and(data("recent_distance")(_p)(_a).ne(null)),data("recent_distance")(_p)(_a), 0).do (recent_weight) ->
                r.branch(data("limit_distance")(_p).ne(null).and(data("limit_distance")(_p)(_a).ne(null)),data("limit_distance")(_p)(_a), 0).do (event_weight) ->
                  r.object(_p,r.object(_a, recent_weight.mul(4).add(event_weight.mul(1)).div(5)))
          )
      ).reduce((a,b) ->
          a.merge(b)
      )
    )
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
        promises.push @_r.table("#{namespace}_events").getAll([action,thing], {index: "action_thing"}).orderBy(@_r.desc("created_at")).skip(trunc_size).delete().run({useOutdated: true, durability: "soft"})

    bb.all(promises)


  truncate_people_per_action: (namespace, people, trunc_size, actions) ->
    #TODO do the same thing for things
    return bb.try( -> []) if people.length == 0
    promises = []
    for person in people
      for action in actions
        promises.push @_r.table("#{namespace}_events").getAll([person, action],{index: "person_action"}).orderBy(@_r.desc("created_at")).skip(trunc_size).delete().run({useOutdated: true,durability: "soft"})
    #cut each action down to size
    bb.all(promises)

  remove_events_till_size: (namespace, number_of_events) ->
    #TODO move too offset method
    #removes old events till there is only number_of_events left
    @_r.table("#{namespace}_events").orderBy({index: @_r.desc("created_at")})
    .skip(number_of_events).delete().run({useOutdated: true,durability: "soft"})

  ###########################################
  ####     END Compact Function          ####
  ###########################################

module.exports = RethinkDBESM;
