CREATE TABLE "Config"
(
	"key" text NOT NULL,
	"value" text,
	PRIMARY KEY ("key")
);

CREATE TABLE "Code"
(
  "code" integer NOT NULL,
  "value" text NOT NULL UNIQUE,
  PRIMARY KEY ("code")
);

CREATE TABLE "Target"
(
	"id" integer NOT NULL,
	"parent" integer,
	"type_code" integer NOT NULL,
	"name" text,
	"description" text,
	"data" blob,
	PRIMARY KEY ("id"),
	FOREIGN KEY ("parent") REFERENCES "Target"("id"),
	FOREIGN KEY ("type_code") REFERENCES "Code"("code")
);

CREATE INDEX "Target_Parent_Index" ON "Target"("parent");


CREATE TABLE "ConstraintCollection"
(
	"id" integer NOT NULL,
	"experimentId" integer,
	"description" text,
	"data" blob,
	PRIMARY KEY ("id"),
	FOREIGN KEY("experimentId")
	REFERENCES "Experiment" ("id")
);

CREATE TABLE "Constraint"
(
	"constraintCollection" integer,
	"data" blob not null,
	FOREIGN KEY ("constraintCollection") REFERENCES "ConstraintCollection"("id")
);


CREATE TABLE "Algorithm"
(
	"id" integer NOT NULL,
	"name" text,
	PRIMARY KEY("id")
);

CREATE TABLE "Experiment"
(
	"id" integer NOT NULL,
	"executionTime" integer,
	"description" text,
	"timestamp" text,
	"algorithmId" integer NOT NULL,
	PRIMARY KEY ("id"),
	FOREIGN KEY ("algorithmId")
	REFERENCES "Algorithm" ("id")
);

CREATE TABLE "ExperimentParameter"
(
	"experimentId" integer NOT NULL,
	"keyy" text,
	"value" text,
	FOREIGN KEY ("experimentId")
	REFERENCES "Experiment" ("id")
);

CREATE TABLE "Annotation"
(
	"experimentId" integer NOT NULL,
	"tag" text,
	"textt" text,
	FOREIGN KEY ("experimentId")
	REFERENCES "Experiment" ("id")
);