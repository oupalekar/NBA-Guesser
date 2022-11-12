var express = require('express');
var bodyParser = require('body-parser');
var mysql = require('mysql2');
var path = require('path');
var connection = mysql.createConnection({
                host: '34.136.27.137',
                user: 'root',
                password: 'apple123',
                database: 'PickAndRoll'
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
  res.render('index', { title: 'Mark Attendance', data: '', update: '' });
});

app.get('/success', function(req, res) {
         res.send({'message': ""});
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

  // python.stderr.on('data', function(data) {
  //   console.log(`stderr: ${data}`)
  // });

  python.on('exit', (code) => {
    console.log(`Exited with ${code}: ${dataToSend}`)
    res.render('index', {update : dataToSend})
    // res.write(dataToSend);

    // return;
    // res.sendFile
    // res.redirect("/");
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
                        res.render('index', {data: str});
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
                                res.render('index', {data: str});
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
                res.render('index', {data: str});
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
});

app.post('/update', function(req, res) {
});

app.listen(80, function () {
    console.log('Node app is running on port 80');
});