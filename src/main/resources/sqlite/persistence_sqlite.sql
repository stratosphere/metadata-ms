

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


CREATE TABLE [ConstraintCollection]
(
	[id] integer NOT NULL,
	[description] text,
	PRIMARY KEY ([id])
);


CREATE TABLE [Constraintt]
(
	[id] integer NOT NULL,
	[constraintCollectionId] integer NOT NULL,
	PRIMARY KEY ([id]),
	FOREIGN KEY ([constraintCollectionId])
	REFERENCES [ConstraintCollection] ([id])
);


CREATE TABLE [IND]
(
	[constraintId] integer NOT NULL,
	PRIMARY KEY ([constraintId]),
	FOREIGN KEY ([constraintId])
	REFERENCES [Constraintt] ([id])
);


CREATE TABLE [INDpart]
(
	[constraintId] integer NOT NULL,
	[lhs] integer NOT NULL,
	[rhs] integer NOT NULL,
	FOREIGN KEY ([lhs])
	REFERENCES [Columnn] ([id]),
	FOREIGN KEY ([rhs])
	REFERENCES [Columnn] ([id]),
	FOREIGN KEY ([constraintId])
	REFERENCES [IND] ([constraintId])
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


CREATE TABLE [Typee]
(
	[constraintId] integer NOT NULL,
	[columnId] integer NOT NULL,
	[typee] text,
	FOREIGN KEY ([constraintId])
	REFERENCES [Constraintt] ([id]),
	FOREIGN KEY ([columnId])
	REFERENCES [Columnn] ([id])
);



