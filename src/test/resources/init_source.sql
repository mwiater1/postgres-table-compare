CREATE SCHEMA test;

CREATE TABLE test.users
(
    id         BIGINT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name  VARCHAR(50),
    email      VARCHAR(50)
);

INSERT INTO test.users (id, first_name, last_name, email)
VALUES (1, 'Sebastiano', 'Lammers', 'slammers0@loc.gov'),
       (2, 'Vanni', 'Mowlam', 'vmowlam1@npr.org'),
       (3, 'Gerry', 'MacColl', 'gmaccoll2@nps.gov'),
       (4, 'Zenia', 'Reilingen', 'zreilingen3@cocolog-nifty.com'),
       (5, 'Tera', 'Longfellow', 'tlongfellow4@whitehouse.gov'),
       (6, 'Aline', 'McCrossan', 'amccrossan5@biblegateway.com'),
       (7, 'Brooke', 'Muglestone', 'bmugglestone6@imdb.com'),
       (8, 'Tabby', 'Howler', 'thowler7@tinyurl.com'),
       (9, 'Robenia', 'Leggis', 'rleggis8@wisc.edu'),
       (10, 'Cheslie', 'Cullingford', 'ccullingford9@ow.ly');

CREATE TABLE test.users_missing_email
(
    id         BIGINT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name  VARCHAR(50)
);

INSERT INTO test.users_missing_email (id, first_name, last_name)
VALUES (1, 'Sebastiano', 'Lammers'),
       (2, 'Vanni', 'Mowlam'),
       (3, 'Gerry', 'MacColl'),
       (4, 'Zenia', 'Reilinge'),
       (5, 'Tera', 'Longfellow'),
       (6, 'Aline', 'McCrossan'),
       (7, 'Brooke', 'Mugglestone'),
       (8, 'Tabby', 'Howler'),
       (9, 'Robenia', 'Leggis'),
       (10, 'Cheslie', 'Cullingford');

CREATE TABLE test.users_missing_last_name
(
    id         BIGINT PRIMARY KEY,
    first_name VARCHAR(50),
    email      VARCHAR(50)
);

INSERT INTO test.users_missing_last_name (id, first_name, email)
VALUES (1, 'Sebastiano', 'slammers0@loc.gov'),
       (2, 'Vanni', 'vmowlam1@npr.org'),
       (3, 'Gerry', 'gmaccoll2@nps.gov'),
       (4, 'Zenia', 'zreilingen3@cocolog-nifty.com'),
       (5, 'Tera', 'tlongfellow@whitehouse.gov'),
       (6, 'Aline', 'amccrossan5@biblegateway.com'),
       (7, 'Brooke', 'bmugglestone6@imdb.com'),
       (8, 'Tabby', 'thowler7@tinyurl.com'),
       (9, 'Robenia', 'rleggis8@wisc.edu'),
       (10, 'Cheslie', 'ccullingford9@ow.ly');
