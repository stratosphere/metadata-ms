-- This file contains various primary key definitions that should all be supported.

\set ERROR off

CREATE TABLE dep1 (
  id INT PRIMARY KEY,
  that_id INT REFERENCES ref1 (id)
);

CREATE TABLE dep2 (
  that_id INT,
  CONSTRAINT my_fk FOREIGN KEY ("that_id") REFERENCES ref2 (id)
);


CREATE TABLE dep3 (
  first_name VARCHAR(32),
  last_name VARCHAR(32),
  FOREIGN KEY ([first_name], last_name) REFERENCES ref3 (fn, ln)
);

alter table dep4
add column that_oid text references ref4 (`oid`);

alter table dep5 add foreign key (first_name, last_name) references ref5 (fn, ln);

create table table6 (
  not_an_id int,
  also_not_an_id real
);


alter table table6 add column not_an_id int not null;