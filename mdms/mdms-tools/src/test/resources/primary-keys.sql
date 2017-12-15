-- This file contains various primary key definitions that should all be supported.

\set ERROR off

CREATE TABLE table1 (
  `id` INT PRIMARY KEY,
  not_an_id TEXT
);

CREATE TABLE table2 (
  id INT,
  CONSTRAINT my_pk PRIMARY KEY (id)
);


CREATE TABLE table3 (
  first_name VARCHAR(32),
  last_name VARCHAR(32),
  PRIMARY KEY (first_name, [last_name])
);

alter table table4
add column added_id text primary key;

alter table table5 add primary key (added_fn, added_ln);

create table table6 (
  not_an_id int,
  also_not_an_id real
);


alter table table6 add column not_an_id int not null;