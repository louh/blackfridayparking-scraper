'use strict'

var env = require('node-env-file')
var request = require('superagent')
var CartoDB = require('cartodb')

// Read env configs
env(__dirname + '/.env')

var CARTODB_TABLE = process.env.CARTODB_TABLE
var INSTAGRAM_QUERY_ENDPOINT = 'https://api.instagram.com/v1/tags/blackfridayparking/media/recent?client_id='

var START_TIME = getStartTime(process.env.START_TIME)

var client = new CartoDB({
  user: 'lou',
  api_key: process.env.CARTODB_API_KEY
})

client.on('connect', function () {
  console.log('Connected to CartoDB')

  // Instagram
  request
    .get(INSTAGRAM_QUERY_ENDPOINT + process.env.INSTAGRAM_ID)
    .end(function (err, res) {
      if (err) {
        console.log('Error getting instagram response:' + err)
      }

      console.log('[Instagram] Processing data ...')

      var data = res.body.data

      for (var i = 0; i < data.length; i++) {
        var gram = data[i]

        // Only keep posts made after START_TIME
        if (gram.created_time < START_TIME) continue

        // Only keep geocoded posts
        if (!gram.location) continue

        var cartoSQL = "INSERT INTO {table} (the_geom, description, identifier, percent_full, source, source_created_at, url, username) VALUES ({geo},'{description}','{identifier}',{percent_full},'instagram',to_timestamp({source_created_at}),'{url}','{username}')"

        client.query(cartoSQL, {
          table: CARTODB_TABLE,
          geo: 'ST_SetSRID(ST_Point(' + gram.location.longitude + ',' + gram.location.latitude + '),4326)',
          description: gram.caption.text.replace(/'/g, "''"),
          identifier: gram.id,
          percent_full: getPercentFromText(gram.caption.text),
          source_created_at: gram.created_time,
          url: gram.link,
          username: gram.user.username
        }, function (err, data) {
          if (err) {
            // 'identifier' is a UNIQUE column, so errors will occur if you add the same one
            // console.log('[CartoDB] error performing SQL query:', err)
          } else if (data.total_rows) {
            console.log('[Instagram] ' + data.total_rows + ' rows affected')
          }
        })
      }
      console.log('[Instagram] Done.')
    })
  // End Instagram

  // Twitter
  request
    .get('https://api.twitter.com/1.1/search/tweets.json?q=%23blackfridayparking')
    .set('Authorization', 'Bearer ' + process.env.TWITTER_BEARER_TOKEN)
    .end(function (err, res) {
      if (err) {
        console.error('[Twitter] Error @ GET request!', err)
        return
      }

      console.log('[Twitter] Processing data ...')

      var data = res.body.statuses

      // console.log(JSON.stringify(data))

      for (var i = 0; i < data.length; i++) {
        var tweet = data[i]
        var geo

        // Only keep posts with an image
        if (!tweet.entities.media) continue

        // There are different types of coordinates.
        // A place is associated with the tweet and returns a bounding box.
        // This is less exact. Do we even want this?
        if (tweet.place) {
          console.log('[Twitter] Place found, but skipping')
        }
        if (tweet.coordinates) {
          geo = 'ST_SetSRID(ST_Point(' + tweet.coordinates.coordinates[0] + ',' + tweet.coordinates.coordinates[1] + '),4326)'
        }
        // There is a tweet.geo field but it is deprecated.

        // Do not proceed if there is not a point
        if (!geo) continue

        // NOTE: sometimes the points come back as [0,0] which CartoDB will issue an error on
        console.log('[Twitter]', JSON.stringify(tweet.coordinates))

        var cartoSQL = "INSERT INTO {table} (the_geom, description, identifier, percent_full, source, source_created_at, username) VALUES ({geo},'{description}','{identifier}',{percent_full},'twitter','{source_created_at}','{username}')"

        client.query(cartoSQL, {
          table: CARTODB_TABLE,
          geo: geo,
          description: tweet.text.replace(/'/g, "''"),
          identifier: tweet.id_str,
          percent_full: getPercentFromText(tweet.text),
          source_created_at: tweet.created_at,
          username: tweet.user.screen_name
        }, function (err, data) {
          if (err) {
            // 'identifier' is a UNIQUE column, so errors will occur if you add the same one
            // console.log('[CartoDB] error performing SQL query:', err)
          } else if (data.total_rows) {
            console.log('[Twitter] ' + data.total_rows + ' rows affected')
          }
        })
      }

      console.log('[Twitter] Done.')
    })
    // End Twitter

// End CartoDB connection
})

client.connect()

// Returns an integer if there is a percentage
// Returns string 'null' to store in db if no percent found
function getPercentFromText (text) {
  if (!text) return 'null'
  var percent = text.match(/[0-9]*%/)
  var int
  if (percent && percent.length >= 1) {
    int = parseInt(percent[0], 10)
    // In case a match is found but is actually not parseable as integer
    if (isNaN(int) === true) {
      return 'null'
    } else {
      return int
    }
  } else {
    return 'null'
  }
}

// Returns time in unix timestamp format given a string
// 'November 20, 2015' -> 1447995600
function getStartTime (string) {
  var time = new Date(string)
  return Math.floor(time.valueOf() / 1000)
}
