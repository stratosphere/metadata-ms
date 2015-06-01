

/* Create Tables */

CREATE TABLE [LocationType]
(
	[id] integer NOT NULL,
	[className] text NOT NULL UNIQUE,
	PRIMARY KEY ([id])
);


CREATE TABLE [Location]
(
	[id] integer NOT NULL PRIMARY KEY AUTOINCREMENT,
	[typee] integer NOT NULL,
	FOREIGN KEY ([typee])
	REFERENCES [LocationType] ([id])
);


CREATE TABLE [Target]
(
	[id] integer NOT NULL,
	[name] text,
	[locationId] integer,
	[description] text,
	PRIMARY KEY ([id]),
	FOREIGN KEY ([locationId])
	REFERENCES [Location] ([id])
);


CREATE TABLE [Schemaa]
(
	[id] integer NOT NULL,
	PRIMARY KEY ([id]),
	FOREIGN KEY ([id])
	REFERENCES [Target] ([id])
);


CREATE TABLE [Tablee]
(
	[id] integer NOT NULL,
	[schemaId] integer NOT NULL,
	PRIMARY KEY ([id]),
	FOREIGN KEY ([schemaId])
	REFERENCES [Schemaa] ([id]),
	FOREIGN KEY ([id])
	REFERENCES [Target] ([id])
);


CREATE TABLE [Columnn]
(
	[id] integer NOT NULL,
	[tableId] integer NOT NULL,
	PRIMARY KEY ([id]),
	FOREIGN KEY ([tableId])
	REFERENCES [Tablee] ([id]),
	FOREIGN KEY ([id])
	REFERENCES [Target] ([id])
);


CREATE TABLE [Config]
(
	[keyy] text NOT NULL,
	[value] text,
	PRIMARY KEY ([keyy])
);

CREATE TABLE [Algorithm]
(
	[id] integer NOT NULL,
	[name] text,
	PRIMARY KEY([id])
);

CREATE TABLE [Experiment]
(
	[id] integer NOT NULL,
	[executionTime] integer,
	[description] text,
	[timestamp] text,
	[algorithmId] integer NOT NULL,
	PRIMARY KEY ([id]),
	FOREIGN KEY ([algorithmId])
	REFERENCES [Algorithm] ([id])
);

CREATE TABLE [ExperimentParameter]
(
	[experimentId] integer NOT NULL,
	[keyy] text,
	[value] text,
	FOREIGN KEY ([experimentId])
	REFERENCES [Experiment] ([id])
);

CREATE TABLE [Annotation]
(
	[experimentId] integer NOT NULL,
	[tag] text,
	[textt] text,
	FOREIGN KEY ([experimentId])
	REFERENCES [Experiment] ([id])
);

CREATE TABLE [ConstraintCollection]
(
	[id] integer NOT NULL,
	[experimentId] integer,
	[description] text,
	PRIMARY KEY ([id]),
	FOREIGN KEY([experimentId])
	REFERENCES [Experiment] ([id])
);


CREATE TABLE [Constraintt]
(
	[id] integer NOT NULL,
	[constraintCollectionId] integer NOT NULL,
	PRIMARY KEY ([id]),
	FOREIGN KEY ([constraintCollectionId])
	REFERENCES [ConstraintCollection] ([id])
);

CREATE TABLE [LocationProperty]
(
	[locationId] integer NOT NULL,
	[keyy] text,
	[value] text,
	FOREIGN KEY ([locationId])
	REFERENCES [Location] ([id])
);


CREATE TABLE [Scope]
(
	[targetId] integer NOT NULL,
	[constraintCollectionId] integer NOT NULL,
	FOREIGN KEY ([constraintCollectionId])
	REFERENCES [ConstraintCollection] ([id]),
	FOREIGN KEY ([targetId])
	REFERENCES [Target] ([id])
);
