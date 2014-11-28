'use strict';

var env     = require('node-env-file'),
    request = require('superagent'),
    btoa    = require('btoa'),
    CartoDB = require('cartodb')

// Read env configs
//env(__dirname + '/.env')

var CARTODB_TABLE            = 'blackfridayparking',
    INSTAGRAM_QUERY_ENDPOINT = 'https://api.instagram.com/v1/tags/blackfridayparking/media/recent?client_id='

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

        // Only keep geocoded posts
        if (!gram.location) continue

        var cartoSQL = "INSERT INTO {table} (the_geom, identifier, percent_full, source, source_created_at, url, username) VALUES ({geo},'{identifier}',{percent_full},'instagram',to_timestamp({source_created_at}),'{url}','{username}')"

        client.query(cartoSQL, {
          table: CARTODB_TABLE,
          geo: 'ST_SetSRID(ST_Point('+gram.location.longitude+','+gram.location.latitude+'),4326)',
          identifier: gram.id,
          percent_full: getPercentFromText(gram.caption.text),
          source_created_at: gram.created_time,
          url: gram.link,
          username: gram.user.username
        }, function (err, data) {
          if (err) {
            // 'identifier' is a UNIQUE column, so errors will occur if you add the same one
            //console.log('[CartoDB] error performing SQL query:', err)
          } else if (data.total_rows) {
            console.log('[Instagram] ' + data.total_rows + ' rows affected')
          }
        })
      }
      console.log('[Instagram] Done.')
    })
  // End Instagram

  // WTF Twitter
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

      //console.log(JSON.stringify(data))

      for (var i = 0; i < data.length; i++) {
        var tweet = data[i]

        // Only keep geocoded posts
        if (!tweet.coordinates) continue
        // Only keep posts with an image
        if (!tweet.entities.media) continue

        // NOTE: sometimes the points come back as [0,0] which CartoDB will issue an error on
        console.log('[Twitter]', JSON.stringify(tweet.coordinates))

        var cartoSQL = "INSERT INTO {table} (the_geom, identifier, percent_full, source, source_created_at, username) VALUES ({geo},'{identifier}',{percent_full},'twitter','{source_created_at}','{username}')"

        client.query(cartoSQL, {
          table: CARTODB_TABLE,
          geo: 'ST_SetSRID(ST_Point('+tweet.coordinates.coordinates[0]+','+tweet.coordinates.coordinates[1]+'),4326)',
          identifier: tweet.id_str,
          percent_full: getPercentFromText(tweet.text),
          source_created_at: tweet.created_at,
          username: tweet.user.screen_name
        }, function (err, data) {
          if (err) {
            // 'identifier' is a UNIQUE column, so errors will occur if you add the same one
            //console.log('[CartoDB] error performing SQL query:', err)
          } else if (data.total_rows) {
            console.log('Twitter] ' + data.total_rows + ' rows affected')
          }
        })
      }

      console.log('[Twitter] Done.')
    })

// End CartoDB connection
})

client.connect()

function getPercentFromText (text) {
  if (!text) return null
  var percent = text.match(/[0-9]*%/)
  if (percent && percent.length >= 1) return parseInt(percent[0])
  else return null
}


// WTF Twitter
// curl -u API_KEY:SECRET_KEY -d grant_type=client_credentials https://api.twitter.com/oauth2/token
/*
var token = btoa(process.env.TWITTER_API_KEY + ':' + process.env.TWITTER_SECRET_KEY)
request
  .post('https://api.twitter.com/oauth2/token')
  .set('Authorization', 'Basic ' + token)
  .set('Content-Type', 'application/x-www-form-urlencoded;charset=UTF-8')
  .send('grant_type=client_credentials')
  .end(function (err, res) {
    if (err) {
      console.error('Error @ POST request!', err)
    }

    console.log(res.body)
  })
*/
