const mysql = require('mysql');

const db = mysql.createConnection({
    host: 'localhost',
    user: 'root',
    password: '0203',
    database: 'ottplatform'
});

db.connect((err) => {
    if(err){
        console.log('this is the ', err);
        throw err;
    }
    console.log('MySql Connected...');
});




// create table if not existss
let User = `create table if not exists User (user_id INT PRIMARY KEY auto_increment, name VARCHAR(255), email VARCHAR(255) unique, phone_no VARCHAR(20), subscription_status ENUM('active', 'inactive') DEFAULT 'inactive') ENGINE=InnoDB;`;

let Content = `create table if not exists Content (content_id INT PRIMARY KEY AUTO_INCREMENT, content_title VARCHAR(255) unique, content_type ENUM('movie', 'web series')) ENGINE=InnoDB;`;

let Movie = `create table if not exists Movie (content_id INT PRIMARY KEY, title VARCHAR(255) unique, description TEXT, release_date DATE, duration INT, director VARCHAR(255), production VARCHAR(255), imdb_rating DECIMAL(3, 1), pg_rating VARCHAR(10), country VARCHAR(255), poster_link VARCHAR(255), movie_link VARCHAR(255), trailer_link VARCHAR(255), language VARCHAR(255), FOREIGN KEY (content_id) REFERENCES Content (content_id) ON DELETE CASCADE) ENGINE=InnoDB;`;

let Subscription_Plan = `create table if not exists SubscriptionPlan (plan_id INT PRIMARY KEY, plan_name VARCHAR(255), duration INT, price DECIMAL(10, 2)) ENGINE=InnoDB;`;

let WebSeries = `create table if not exists WebSeries (content_id INT PRIMARY KEY, title VARCHAR(255) unique, description TEXT, release_date DATE, director VARCHAR(255), cast VARCHAR(255), language VARCHAR(255), trailer_link VARCHAR(255), poster_link VARCHAR(255), number_of_episodes INT, pg_rating VARCHAR(10), imdb_rating DECIMAL(3, 1), FOREIGN KEY (content_id) REFERENCES Content (content_id) ON DELETE CASCADE) ENGINE=InnoDB;`;

let Seasons = `CREATE TABLE if not exists Seasons (season_id INT PRIMARY KEY, content_id INT, season_number INT, FOREIGN KEY (content_id) REFERENCES Content(content_id) ON DELETE CASCADE) ENGINE=InnoDB;`;

let Episodes = `CREATE TABLE if not exists Episodes (episode_id INT PRIMARY KEY Auto_increment, season_id INT, title VARCHAR(255), description TEXT, release_date DATE, duration TIME, thumbnail VARCHAR(255), imdb_rating DECIMAL(3,1), trailer_link VARCHAR(255), FOREIGN KEY (season_id) REFERENCES Seasons(season_id) ON DELETE CASCADE) ENGINE=InnoDB;`;

let Subscription = `create table if not exists Subscription (subscription_id INT PRIMARY KEY auto_increment, user_id INT, plan_id INT, start_date DATE, end_date DATE, FOREIGN KEY (user_id) REFERENCES User(user_id) ON DELETE CASCADE, FOREIGN KEY (plan_id) REFERENCES SubscriptionPlan(plan_id) ON DELETE CASCADE) ENGINE=InnoDB;`;

let Watchlist = `create table if not exists Watchlist (watchlist_id INT PRIMARY KEY auto_increment, user_id INT, content_id INT, episode_id INT, added_date DATE, FOREIGN KEY (user_id) REFERENCES User(user_id) ON DELETE CASCADE, FOREIGN KEY (content_id) REFERENCES Content(content_id) ON DELETE CASCADE, FOREIGN KEY (episode_id) REFERENCES Episodes(episode_id) ON DELETE CASCADE) ENGINE=InnoDB;`;

let Playback = `create table if not exists Playback (playback_id INT PRIMARY KEY auto_increment, user_id INT, content_id INT, season_id INT, episode_id INT, playback_position TIME, FOREIGN KEY (user_id) REFERENCES User(user_id) ON DELETE CASCADE, FOREIGN KEY (content_id) REFERENCES Content(content_id) ON DELETE CASCADE) ENGINE=InnoDB;`;

let Rating = `create table if not exists Rating (rating_id INT PRIMARY KEY auto_increment, user_id INT, content_id INT, episode_id INT, rating INT, review TEXT, FOREIGN KEY (user_id) REFERENCES User(user_id) ON DELETE CASCADE, FOREIGN KEY (content_id) REFERENCES Content(content_id) ON DELETE CASCADE, FOREIGN KEY (episode_id) REFERENCES Episodes(episode_id) ON DELETE CASCADE) ENGINE=InnoDB;`;

let Genre = `create table if not exists Genre (genre_id INT PRIMARY KEY, genre_name VARCHAR(255)) ENGINE=InnoDB;`;

let Cast = `create table if not exists Casts (cast_id INT AUTO_INCREMENT PRIMARY KEY, actor_name VARCHAR(255), content_id INT, FOREIGN KEY (content_id) REFERENCES Content (content_id) ON DELETE CASCADE) ENGINE=InnoDB;`;

let Content_Genre = `create table if not exists ContentGenre (content_id INT, genre_id INT, PRIMARY KEY (content_id, genre_id), FOREIGN KEY (content_id) REFERENCES Content (content_id) ON DELETE CASCADE, FOREIGN KEY (genre_id) REFERENCES Genre (genre_id) ON DELETE CASCADE);`;


// Inserting data into Genre table
let insert_genre = `INSERT ignore INTO Genre (genre_id, genre_name) VALUES (1, 'Action'), (2, 'Adventure'),(3, 'Comedy'), (4, 'Crime'),(5, 'Drama'),(6, 'Fantasy'),(7, 'Historical'),(8, 'Horror'),(9, 'Mystery'),(10, 'Political'),(11, 'Romance'),(12, 'Science Fiction'),(13, 'Thriller'),(14, 'Western');`

let insert_subscription_plans = `INSERT ignore INTO SubscriptionPlan (plan_id, plan_name, duration, price) VALUES (1, 'Basic', 1, 99.00), (2, 'Standard', 3, 249.00), (3, 'Premium', 6, 499.00);`

//Triggers

let trigger_user_subscription = `CREATE TRIGGER if not exists update_subscription_status
BEFORE INSERT ON Subscription
FOR EACH ROW
BEGIN
    IF NEW.end_date < CURRENT_DATE THEN
        UPDATE User
        SET subscription_status = 'inactive'
        WHERE user_id = NEW.user_id;
    END IF;
END;
`
let trigger_subscription_dates = `CREATE TRIGGER if not exists set_subscription_dates
BEFORE INSERT ON Subscription
FOR EACH ROW
BEGIN
    DECLARE plan_duration INT;
    
    -- Get the duration of the selected plan
    SELECT duration INTO plan_duration
    FROM SubscriptionPlan
    WHERE plan_id = NEW.plan_id;
    
    -- Set the start date as the current date
    SET NEW.start_date = CURDATE();
    
    -- Calculate and set the end date based on the start date and plan duration
    SET NEW.end_date = DATE_ADD(NEW.start_date, INTERVAL plan_duration MONTH);
END;
`


let initialQueries = [User, Content, Movie, Subscription_Plan, WebSeries, Seasons, Episodes, Subscription, Watchlist, Playback, Rating, Genre, Cast, Content_Genre, insert_genre, trigger_user_subscription, trigger_subscription_dates, insert_subscription_plans];

initialQueries.forEach((query) => {
    db.query(query, (err, result) => {
        if(err) throw err;
        // console.log(result);
    });
})


let genre_indexing = `CREATE INDEX idx_contentgenre_content_id ON ContentGenre (content_id);`

let cast_indexing =  `CREATE INDEX idx_casts_content_id ON Casts (content_id);`

let rating_indexing =  `CREATE INDEX idx_rating_user_id_content_id_episode_id ON Rating (user_id, content_id, episode_id);`

let playback_indexing =  `CREATE INDEX idx_playback_user_id_content_id ON Playback (user_id, content_id);`

let watchlist_indexing =  `CREATE INDEX idx_watchlist_user_id_content_id ON Watchlist (user_id, content_id);`

let subscription_indexing =  `CREATE INDEX idx_subscription_user_id ON Subscription (user_id);`


let indexingQueries = [genre_indexing, cast_indexing, rating_indexing, playback_indexing, watchlist_indexing, subscription_indexing];

indexingQueries.forEach((query) => {
    db.query(query, (err, result) => {
        if(err) throw err;
        // console.log(result);
    });
});

//Partitioning movie table according to release date
let movie_partitioning = `ALTER TABLE Movie PARTITION BY RANGE (YEAR(release_date)) (PARTITION p0 VALUES LESS THAN (2010), PARTITION p1 VALUES LESS THAN (2020), PARTITION p2 VALUES LESS THAN MAXVALUE);`

// Partitioning webseries table according to release date
let webseries_partitioning = `ALTER TABLE WebSeries PARTITION BY RANGE (YEAR(release_date)) (PARTITION p0 VALUES LESS THAN (2010), PARTITION p1 VALUES LESS THAN (2020), PARTITION p2 VALUES LESS THAN MAXVALUE);`

// Partitioning episodes table according to episode_id
let episodes_partitioning = `
ALTER TABLE Episodes PARTITION BY RANGE (episode_id) (PARTITION p1 VALUES LESS THAN (1000), PARTITION p2 VALUES LESS THAN (2000), PARTITION p3 VALUES LESS THAN (3000), PARTITION p4 VALUES LESS THAN (MAXVALUE));`





function addMovie(name, type, genre, director, cast, description, release_date, duration, production, imdb_rating, pg_rating, country, poster_link, trailer_link, movie_link, language){
  let genreVal = genre.map((genre) => `'${genre}'`).join(', ');
  let startTransactionQuery = `START TRANSACTION;`;
  let insertContentQuery = `INSERT INTO Content (content_title, content_type) VALUES ('${name}', 'movie');`;
  let getContentIdQuery = `SET @content_id = LAST_INSERT_ID();`;
  let insertMovieQuery = `INSERT INTO movie (content_id, title, description, release_date, duration, director, production, imdb_rating, pg_rating, country, poster_link, movie_link,trailer_link) VALUES (@content_id, '${name}', '${description}', '${release_date}', '${duration}', '${director}', '${production}', '${imdb_rating}', '${pg_rating}', '${country}', '${poster_link}', '${movie_link}','${trailer_link}');`;
  let insertContentGenreQuery = `INSERT INTO contentgenre (content_id, genre_id) SELECT @content_id, genre_id FROM genre WHERE genre_name IN (${genreVal});`;

  let insertCastQuery = `INSERT INTO Casts (actor_name,content_id) VALUES `;
  for(let i=0;i<cast.length-1;i++){
    insertCastQuery += `('${cast[i]}', @content_id), `;
  }
  insertCastQuery += `('${cast[cast.length-1]}', @content_id); `;
  
  // let insertCastQuery = `INSERT INTO casts (actor_name, content_id) VALUES ('Leonardo DiCaprio', @content_id), ('Joseph Gordon-Levitt', @content_id), ('Elliot Page', @content_id);`;
  let commitQuery = `COMMIT;`;

// Execute the queries separately
  db.query(startTransactionQuery);
  db.query(insertContentQuery);
  db.query(getContentIdQuery);
  db.query(insertMovieQuery);
  db.query(insertContentGenreQuery);
  db.query(insertCastQuery);
  db.query(commitQuery);
}


function addWebSeries(name, genre, director, cast, description, release_date, duration, production, imdb_rating, pg_rating, country, poster_link, trailer_link, movie_link, language){
  let genreVal = genre.map((genre) => `${genre}`).join(', ');
  let startTransactionQuery = `START TRANSACTION;`;
  let insertContentQuery = `INSERT INTO Content (content_title, content_type) VALUES (${name}, 'web series');`;
  let getContentIdQuery = `SET @content_id = LAST_INSERT_ID();`;
  let insertMovieQuery = `INSERT INTO WebSeries (content_id, title, description, release_date, duration, director, production, imdb_rating, pg_rating, country, poster_link, movie_link,trailer_link) VALUES (@content_id, ${name}, ${description}, ${release_date}, ${duration}, ${director}, ${production}, ${imdb_rating}, ${pg_rating}, ${country}, ${poster_link}, ${movie_link},${trailer_link});`;
  let insertContentGenreQuery = `INSERT INTO contentgenre (content_id, genre_id) SELECT @content_id, genre_id FROM genre WHERE genre_name IN (${genreVal});`;
  let insertCastQuery = `INSERT INTO Casts (actor_name,content_id) VALUES `; 
  for(let i=0;i<cast.length;i++){
    insertCastQuery += `(${cast[i]}, @content_id) `;
  }
  insertCastQuery +=';'
  // let insertCastQuery = `INSERT INTO casts (actor_name, content_id) VALUES ('Leonardo DiCaprio', @content_id), ('Joseph Gordon-Levitt', @content_id), ('Elliot Page', @content_id);`;
  let commitQuery = `COMMIT;`;
  db.query(startTransactionQuery);
  db.query(insertContentQuery);
  db.query(getContentIdQuery);
  db.query(insertMovieQuery);
  db.query(insertContentGenreQuery);
  db.query(insertCastQuery);
  db.query(commitQuery);
}

async function showSubscriptionPlans(){
  try {
    // Execute query using async/await and return the result
    let sql = `SELECT * FROM SubscriptionPlan;`;
    const results = await executeQuery(sql);
    return results;
  } catch (error) {
    console.error(error);
    throw error;
  } finally {
    // Close the database connection
    db.end();
  }
}

function buySubscription(user_id,plan_id){
  let sql = `INSERT INTO Subscription (user_id, plan_id) VALUES (${user_id}, ${plan_id});`;
  db.query(sql,(err,result)=>{
    if(err) throw err;
    return result;
  });
}


function createNewUser(name,email,phone_no){
  let sql = `INSERT INTO User (name, email, phone_no) VALUES ('${name}', '${email}', '${phone_no}');`;
  db.query(sql,(err,result)=>{
    if(err) throw err;
    return result;
  })
}

function addToWatchlist(user, content){
  let sql = `INSERT INTO Watchlist (user_id, content_id, added_date) VALUES ((select user_id from User where user_name = ${user}), (select content_id from Content where Content_name = ${content}), CURDATE());`;
  db.query(sql,(err,result)=>{
    if(err) throw err;
  });
}

function removeFromWatchlist(user, content){
  let sql = `DELETE FROM Watchlist WHERE user_id = ((select user_id from User where user_name = ${user}), content_id = (select content_id from Content where Content_name = ${content}));`;
  db.query(sql,(err,result)=>{
    if(err) throw err;
  });
}

function savePlayback(...args){
  if(args.length != 3 && args.length != 5){
    console.log("Invalid number of arguments");
    return;
  }
  else if(args.length == 3){
    let sql = `INSERT INTO Playback (user_id, content_id, playback_position) VALUES ((select user_id from User where user_name = ${args[0]}), (select content_id from Content where Content_name = ${args[1]}), ${args[2]});`;
    
  }
  else{
    let sql = `INSERT INTO Playback (user_id, content_id, season_number, episode_id,playback_position) VALUES ((select user_id from User where user_name = ${args[0]}), (select content_id from Content where Content_name = ${args[1]}), ${args[2]}, ${args[3]}, ${args[4]});`;
  }
  db.query(sql,(err,result)=>{
    if(err) throw err;
    return result;
  });
  
}

async function executeQuery(sql) {
  return new Promise((resolve, reject) => {
    db.query(sql, (error, results) => {
      if (error) {
        reject(error);
      } else {
        resolve(results);
      }
    });
  });
}

// async function watch(content){
//   let sql = `SELECT * FROM Movie WHERE content_id = (select content_id from Content where content_title='${content}');`;
//   let result;
//   try{
//     result = await executeQuery(sql);
//     console.log(result)
//     result.then((res)=>{return(res)});
//   }
//   catch(err){
//     console.log(err);
//   }
  
  
// }


async function watch(content) {
  try {
    // Execute query using async/await and return the result
    let sql = `SELECT * FROM Movie WHERE content_id = (select content_id from Content where content_title='${content}');`;
    const results = await executeQuery(sql);
    return results;
  } catch (error) {
    console.error(error);
    throw error;
  } finally {
    // Close the database connection
    db.end();
  }
}


function rate(user, content, rating, review){
  let sql = `INSERT INTO Rating (user_id, content_id, rating, review) VALUES ((select user_id from User where user_name = ${user}), (select content_id from Content where Content_name = ${content}), ${rating}, ${review});`;
  db.query(sql,(err,result)=>{
    if(err) throw err;
    return 'rated successfully';
  });
}

function showWatchlist(user){
  let sql = `SELECT * FROM Watchlist WHERE user_id = (select user_id from User where user_name = ${user});`;
  db.query(sql,(err,result)=>{
    if(err) throw err;
    return result;
  });
}
// removeMovies();
// addMovie('Inception', 'movie', ['Action', 'Adventure', 'Sci-Fi'], 'Christopher Nolan', ['Leonardo DiCaprio', 'Joseph Gordon-Levitt', 'Elliot Page'], 'A thief who steals corporate secrets through the use of dream-sharing technology is given the inverse task of planting an idea into the mind of a C.E.O.', '2010-07-16', 148, 'Warner Bros. Pictures', 8.8, 'PG-13', 'USA', 'https://www.imdb.com/title/tt1375666/mediaviewer/rm4039471360/', 'https://www.imdb.com/title/tt1375666/videoplayer/vi3877612057?ref_=tt_ov_vi', 'https://www.imdb.com/title/tt1375666/videoplayer/vi3877612057?ref_=tt_ov_vi', 'English');


function removeMovies(content){
  let sql = `DELETE FROM Movie WHERE title = ${content};`;
  // let sql1 = `DELETE FROM Content WHERE content_title = 'Inception';`;

  db.query(sql,(err,result)=>{
    if(err) throw err;
  });
}

// addMovie('Inception', 'movie', ['Action', 'Adventure', 'Sci-Fi'], 'Christopher Nolan', ['Leonardo DiCaprio', 'Joseph Gordon-Levitt', 'Elliot Page'], 'A thief who steals corporate secrets through the use of dream-sharing technology is given the inverse task of planting an idea into the mind of a C.E.O.', '2010-07-16', 148, 'Warner Bros. Pictures', 8.8, 'PG-13', 'USA', 'https://www.imdb.com/title/tt1375666/mediaviewer/rm4039471360/', 'https://www.imdb.com/title/tt1375666/videoplayer/vi3877612057?ref_=tt_ov_vi', 'https://www.imdb.com/title/tt1375666/videoplayer/vi3877612057?ref_=tt_ov_vi', 'English');
// watch('Inception').then((res)=>{console.log(res)});
// removeMovies('Inception');
// showSubscriptionPlans().then((res)=>{console.log(res)});
// createNewUser('Rahul','rahul@gmail.com,','1234567890');
// buySubscription(1,1);







