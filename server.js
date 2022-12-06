const {spawn} = require('child_process');
var express = require('express');
var bodyParser = require('body-parser');
var mysql = require('mysql2');
var path = require('path');
var connection = mysql.createConnection({
                host: '34.136.27.137',
                user: 'root',
                password: 'apple123',
                database: 'PickAndRoll',
                multipleStatements: true
});

connection.connect;


var app = express();

// set up ejs view engine
app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'ejs');

app.use(express.json());
app.use(express.urlencoded({ extended: false }));
app.use(express.static(__dirname + '../public'));

/* GET home page, respond by rendering index.ejs */
app.get('/', function(req, res) {
  var player_id;
  const game = spawn('python3', ['game.py']);
  game.stdout.on('data', (data) => {
    // console.log(data.toString());
    var player_id = data.toString().split('\n')[1];
    var stat = data.toString().split('\n')[2];
    var stat_value = data.toString().split('\n')[3];
    var year = data.toString().split('\n')[4];
    var team_wins = data.toString().split('\n')[5];
    var max_pts = data.toString().split('\n')[6];
    var name_sql = `SELECT FirstName, LastName FROM Player WHERE PlayerID = ${player_id};`;
    console.log(name_sql)    
    connection.query(name_sql, (err, result) => {
      if (err){throw err;}        
      console.log(result[0]['FirstName'], result[0]['LastName']);    
      var query = `call DoubleDoubleRate("${result[0]['FirstName']}", "${result[0]['LastName']}");`
      console.log(query);
      global.hints = {first_name: result[0]['FirstName'], last_name: result[0]['LastName'], player_id: player_id, stat_param: stat, value_param : stat_value, year_param: year, team_wins_param: team_wins, max_pts_param: max_pts, guesses: 5, correct: false, lose : false}
      connection.query(query, function(err, result){
      if(err){
        console.log(err)
      }
      console.log(result[0])
      var rate = result[0][0]['percentage']
      global.hints.double = rate;
    res.render('guesser', global.hints);
    })
  });
});
});

app.post('/', function(req, res) {
  var player_name = req.body.player_name.split(' ')
  var first_name = player_name[0]
  if (player_name.length == 3) {
    var last_name = player_name[1] + " " + player_name[2]
  } else {
    var last_name = player_name[1]
  }
  console.log(player_name, first_name, last_name, global.hints.first_name, global.hints.last_name)
  console.log(first_name == global.hints.first_name)
  console.log(last_name == global.hints.last_name)
  if (first_name == global.hints.first_name && last_name == global.hints.last_name) {
    console.log("In True")
    global.hints.correct = true
  }
  // console.log(global.hints[guesses])
  if (global.hints.guesses > 0) {
    global.hints.guesses = global.hints.guesses - 1
  }
  
  if (global.hints.guesses == 0) {
    global.hints.lose = true
  }
  res.render('guesser', global.hints)
});


app.get('/success', function(req, res) {
         res.send({'message': ""});
});

app.get('/index', function(req, res) {
         res.render('index', { data: '', update: '' });
});

app.get('/predictions', function(req, res) {
        res.render('prediction', {output: '', year: '', first_team_name: '', second_team_name: ''});
});

// this code is executed when a user clicks the form submit button
app.post('/get_player_info', function(req, res) {
  var name = req.body.player_name;
  var first_last = name.trim().split(" ")
  console.log(first_last);

  if (first_last.length == 2) {
        var sql = `SELECT * FROM Player WHERE FirstName LIKE '${first_last[0]}%' OR LastName LIKE '${first_last[1]}%';`;
  } else {
        var sql = `SELECT * FROM Player WHERE FirstName LIKE '${first_last[0]}%' OR LastName LIKE '${first_last[0]}%';`;
  }

console.log(sql);
  connection.query(sql, function(err, result) {
    if (err) {
      res.send(err)
      return;
    }
   // res.redirect('/success');
    res.send(result);
  });
});


app.post('/insert_org', function(req, res) {
  var org_name = req.body.org_name;
  var org_abbv = org_name.substring(0, 3);

  var org_count_query = `Running org count query`;
  console.log(org_count_query);
  var org_count_sql = `SELECT COUNT(*) AS c FROM Organization;`;

  connection.query(org_count_sql, function(err1, result1) {
    if (err1) {
      throw err1;
      return;
    }
    console.log(result1);
    var org_count = result1[0].c;
    var org_id = org_count + 1;
    var create_org_sql = `INSERT INTO Organization VALUES("${org_id}","${org_abbv}",  "${org_name}");`;
    console.log("converted to int");
     connection.query(create_org_sql, function(err2, result2) {
        if (err2) {
                throw err2;
                return;
        }
        res.send('Successfully added organization');
    });
  });
});

app.post('/delete_org', function(req, res) {
  var org_name = req.body.org_name;

  var delete_org_sql = `DELETE FROM Organization WHERE OrgName = "${org_name}";`;

  connection.query(delete_org_sql, function(err1, result1) {
    if (err1) {
      throw err1;
      return;
    }
    console.log(result1);
    res.send('Successfully deleted organization');
  });
});

app.post('/update', function(req, res) {
  console.log("Clicked")
  var dataToSend;
  const python = spawn("python3", ['update.py'])

  python.stdout.on('data', function(data) {
    dataToSend = data.toString()
  });
  console.log(dataToSend)
  // python.stderr.on('data', function(data) {
  //   console.log(`stderr: ${data}`)
  // });

  python.on('event', (code) => {
    console.log(` ${dataToSend}`)
    // res.render('index', {update : dataToSend, data: ''})
    // res.write(dataToSend);

    // return;
    // res.sendFile
    // res.redirect("/");
  })

  python.on('exit', (code) => {
    console.log(`Exited with code ${code}: ${dataToSend}`)
    res.render('index', {update: dataToSend, data: ''})
  })

});

app.post('/delete', function(req, res) {
  var org_name = req.body.org_name;
  var org_abbv = org_name.substring(3);

  var delete_org_sql = `DELETE FROM Organization WHERE OrgName = "${org_name}";`;

  connection.query(delete_org_sql, function(err1, result1) {
    if (err1) {
      throw err1;
      return;
    }
    console.log(result1);
    res.send('Successfully deleted organization');
  });
});

function createButtons(arr) {
  for (let i = 0; i < arr.length; i++) {
        let btn = document.createElement("button");
        button.innerHTML = arr[i]["FirstName"] + " " + arr[i]["LastName"];
        btn.setAttribute("class", "btn btn-primary");
        document.body.appendChild(btn);
  }
}

function stringSeparate(arr) {
        str = "";
        for (let  i = 0; i < arr.length; i++) {
                str = str + arr[i]["FirstName"] + " " + arr[i]["LastName"];
                if (i != arr.length - 1) {
                        str = str + ", "
                }
        }
        return str;
}

app.post('/guess', function(req, res) {
  var name = req.body.player_name;
  var first_last = name.trim().split(" ")
  console.log(first_last);

  var sql_1 = `SELECT PlayerId, FirstName, LastName FROM Player WHERE FirstName = '${first_last[0]}' AND LastName LIKE '${first_last[1]}%';`;
  var sql_2 = `SELECT PlayerId, FirstName, LastName FROM Player WHERE FirstName LIKE '${first_last[0]}%' OR LastName LIKE '${first_last[0]}%';`;
  var sql_3 = `SELECT PlayerId, FirstName, LastName FROM Player WHERE FirstName = '${first_last[0]}' AND LastName = '${first_last[1]}'`;

  if (first_last.length == 2) {
        connection.query(sql_3, function(err_3, result_3) {
                if (err_3) {
                        res.send(err_3);
                        return;
                }
                if (result_3.length == 1) {
                        console.log("result_3 == 1");
                        // res.send(result_3);
                        // createButtons(result_3);
                        // console.log("hello");
                        var str = stringSeparate(result_3);
                        res.render('index', {data: str, update: ''});
                } else {
                        console.log(sql_3);
                        console.log(result_3);
                        console.log("result_3 != 1");
                        connection.query(sql_1, function(err_1, result_1) {
                                if (err_1) {
                                        res.send(err);
                                        return;
                                }
                                // res.send(result_1);
                                // createButtons(result_1);
                                var str = stringSeparate(result_1);
                                res.render('index', {data: str, update: ''});
                        })
                }
        })
  } else {
        connection.query(sql_2, function(err_2, result_2) {
                if (err_2) {
                        res.send(err);
                        return;
                }
                // res.send(result_2);
                // createButtons(result_2);
                var str = stringSeparate(result_2);
                res.render('index', {data: str, update: ''});
        })
  }
});

app.post('/team_wins', function(req, res) {
  var name = req.body.org_name;
  var year = req.body.year;

  var sql_2 = `SELECT o1.OrgId FROM Team t1 JOIN Organization o1 USING(OrgId) WHERE o1.OrgName = '${name}'`;

  connection.query(sql_2, function(err_2, result_2) {
        if (err_2) {
                res.send(err_2);
                return;
        }
        console.log(result_2);
        var org_id = result_2[0]["OrgId"];
        var sql_3 = `SELECT t1.TeamId FROM Team t1 WHERE t1.OrgId = '${org_id}' AND t1.Year = '${year}'`;
 
        connection.query(sql_3, function(err_3, result_3) {
                if (err_3) {
                        res.send(err_3);
                        return;
                }
                console.log(result_3);
                var team_id = result_3[0]["TeamId"];
                console.log(team_id);
                var sql_1 = `SELECT COUNT(*) FROM (SELECT g1.GameId FROM Team t1 JOIN Game g1 ON(t1.TeamId = g1.HomeTeamId) WHERE (g1.HomeScore > g1.AwayScore AND g1.HomeTeamId = ${team_id} AND t1.Year = ${year}) UNION SELECT g2.GameId FROM Team t2 JOIN Game g2 ON(t2.TeamId = g2.AwayTeamId) WHERE (g2.AwayScore > g2.HomeScore AND g2.AwayTeamId = ${team_id} AND t2.Year = ${year})) AS winning_games;`;
                console.log(sql_1);

                connection.query(sql_1, function(err_1, result_1) {
                        if (err_1) {
                                res.send(err_1);
                                return;
                        }
                        console.log(result_1);
                        var num_wins =  result_1[0]["COUNT(*)"];
                        console.log(num_wins);
                        res.send("" + num_wins);
                })
        })
  })
});

app.post('/player_max_score', function(req, res) {
        var name = req.body.player_name;
        first_last = name.trim().split(" ");
        var year = req.body.year;

        var sql_1 = `SELECT p1.PlayerId FROM Player p1 WHERE p1.FirstName = '${first_last[0]}' AND p1.LastName = '${first_last[1]}'`;

        connection.query(sql_1, function(err_1, result_1) {
                if (err_1) {
                        res.send(err_1);
                        return;
                }

                var player_id = result_1[0]["PlayerId"];
                var sql_2 = `SELECT g1.Date, g1.HomeScore, g1.AwayScore, g1.HomeTeamId, g1.AwayTeamId, b1.Pts FROM Player p1 JOIN BoxScore b1 USING(PlayerId) JOIN Game g1 USING(GameId) JOIN Team t1 ON(t1.TeamId = g1.HomeTeamId) WHERE t1.Year = ${year} AND p1.PlayerId = ${player_id} AND b1.Pts = (SELECT MAX(b2.Pts) FROM Player p2 JOIN BoxScore b2 USING(PlayerId) JOIN Game g2 USING(GameId) JOIN Team t2 ON (t2.TeamId = g2.HomeTeamId) WHERE p2.PlayerId = ${player_id} AND t2.Year = ${year});`

                connection.query(sql_2, function(err_2, result_2) {
                        if (err_2) {
                                res.send(err_2);
                                return;
                        }

                        var info = result_2[0];
                        res.send(info);
                })
        })
});

app.post('/prediction_made', function(req, res) {
    var first_team = req.body.first_team;
    var second_team = req.body.second_team;
    var year = req.body.year;
    console.log(first_team);
    console.log(second_team);
    console.log(year);

    var sql_1 = `SELECT TeamId FROM Team JOIN Organization USING (OrgId) WHERE OrgName = '${first_team}' AND Year = '${year}'`;

    connection.query(sql_1, function(err_1, result_1) {
        if (err_1) {
            res.send(err_1);
            return;
        }
        var first_team_id = result_1[0]["TeamId"];
        console.log(first_team_id);

        var sql_2 = `SELECT TeamId FROM Team JOIN Organization USING (OrgId) WHERE OrgName = '${second_team}' AND Year = '${year}'`;
        
        connection.query(sql_2, function(err_2, result_2) {
            if (err_2) {
                res.send(err_2)
                return;
            }
            var second_team_id = result_2[0]["TeamId"];
            console.log(second_team_id);

            var sp = `CALL ChooseWinner(${first_team_id}, ${second_team_id});`;
            var t1_wins = `SELECT COUNT(*) FROM (SELECT g1.GameId FROM Team t1 JOIN Game g1 ON(t1.TeamId = g1.HomeTeamId) WHERE (g1.HomeScore > g1.AwayScore AND g1.HomeTeamId = ${first_team_id} AND t1.Year = ${year}) UNION SELECT g2.GameId FROM Team t2 JOIN Game g2 ON(t2.TeamId = g2.AwayTeamId) WHERE (g2.AwayScore > g2.HomeScore AND g2.AwayTeamId = ${first_team_id} AND t2.Year = ${year})) AS winning_games;`;
            var t2_wins = `SELECT COUNT(*) FROM (SELECT g1.GameId FROM Team t1 JOIN Game g1 ON(t1.TeamId = g1.HomeTeamId) WHERE (g1.HomeScore > g1.AwayScore AND g1.HomeTeamId = ${second_team_id} AND t1.Year = ${year}) UNION SELECT g2.GameId FROM Team t2 JOIN Game g2 ON(t2.TeamId = g2.AwayTeamId) WHERE (g2.AwayScore > g2.HomeScore AND g2.AwayTeamId = ${second_team_id} AND t2.Year = ${year})) AS winning_games;`;
            var t1_pts = `SELECT SUM(scores.score) FROM ((SELECT SUM(g1.HomeScore) AS score FROM Game g1 WHERE g1.HomeTeamId = ${first_team_id}) UNION (SELECT SUM(g1.AwayScore) AS score FROM Game g1 WHERE g1.AwayTeamId = ${first_team_id})) AS scores;`;
            var t2_pts = `SELECT SUM(scores.score) FROM ((SELECT SUM(g1.HomeScore) AS score FROM Game g1 WHERE g1.HomeTeamId = ${second_team_id}) UNION (SELECT SUM(g1.AwayScore) AS score FROM Game g1 WHERE g1.AwayTeamId = ${second_team_id})) AS scores;`;
            var t1_opp_pts = `SELECT SUM(scores.score) FROM ((SELECT SUM(g1.AwayScore) AS score FROM Game g1 WHERE g1.HomeTeamId = ${first_team_id}) UNION (SELECT SUM(g1.HomeScore) AS score FROM Game g1 WHERE g1.AwayTeamId = ${first_team_id})) AS scores;`;
            var t2_opp_pts = `SELECT SUM(scores.score) FROM ((SELECT SUM(g1.AwayScore) AS score FROM Game g1 WHERE g1.HomeTeamId = ${second_team_id}) UNION (SELECT SUM(g1.HomeScore) AS score FROM Game g1 WHERE g1.AwayTeamId = ${second_team_id})) AS scores;`;
            connection.query(sp + t1_wins + t2_wins + t1_pts + t2_pts + t1_opp_pts + t2_opp_pts, function(err_3, result_3) {
                if (err_3) {
                    res.send(err_3)
                    return;
                }
                console.log(result_3);
                var h2h_winner = result_3[0][0]['team1_wins'];
                var total_winner = result_3[2][0]['COUNT(*)'] > result_3[3][0]['COUNT(*)'] ? 1 : 0;
                var pt_diff_winner = result_3[4][0]['SUM(scores.score)'] - result_3[6][0]['SUM(scores.score)'] > result_3[5][0]['SUM(scores.score)'] - result_3[7][0]['SUM(scores.score)'] ? 1 : 0;
                console.log(h2h_winner);
                console.log(total_winner);
                console.log(pt_diff_winner);
                var t1_score = h2h_winner * 0.4 + total_winner * 0.3 + pt_diff_winner * 0.3;
                var t2_score = 1 - t1_score;
                var winner = t1_score > 0.5 ? first_team : second_team;
                //var percentage = result_3[0][0]['team1_average'] * 100;
                //if (num == 0) {
                  //  winner = second_team;
                   // percentage = 100 - percentage;
                //}
                output = `Based on ${year} data, we predict that ${winner} wins! Our model gives ${first_team} a score of ${t1_score} and ${second_team} a score of ${t2_score}.`;
                res.render('prediction', {output: output, year: year, first_team_name: first_team, second_team_name: second_team});
            })
        })
    })
});

app.post('/crud_function',(req,res)=>{
    res.redirect('/index');
});

app.post('/prediction_function', (req,res)=>{
    res.redirect('/predictions');
});

app.listen(80, function () {
    console.log('Node app is running on port 80');
});