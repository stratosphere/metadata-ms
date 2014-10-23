
/* Drop Tables */

DROP TABLE IF EXISTS [INDpart];
DROP TABLE IF EXISTS [TYPEE];
DROP TABLE IF EXISTS [Columnn];
DROP TABLE IF EXISTS [IND];
DROP TABLE IF EXISTS [Constraintt];
DROP TABLE IF EXISTS [Scope];
DROP TABLE IF EXISTS [ConstraintCollection];
DROP TABLE IF EXISTS [Tablee];
DROP TABLE IF EXISTS [Schemaa];
DROP TABLE IF EXISTS [Target];




/* Create Tables */

CREATE TABLE [Target]
(
	[id] integer NOT NULL,
	[name] text,
	[location] text,
	PRIMARY KEY ([id])
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
	FOREIGN KEY ([id])
	REFERENCES [Target] ([id]),
	FOREIGN KEY ([schemaId])
	REFERENCES [Schemaa] ([id])
);


CREATE TABLE [Columnn]
(
	[id] integer NOT NULL,
	[tableId] integer NOT NULL,
	[locationIndex] integer,
	PRIMARY KEY ([id]),
	FOREIGN KEY ([tableId])
	REFERENCES [Tablee] ([id]),
	FOREIGN KEY ([id])
	REFERENCES [Target] ([id])
);


CREATE TABLE [ConstraintCollection]
(
	[id] integer NOT NULL,
	PRIMARY KEY ([id])
);


CREATE TABLE [Constraintt]
(
	[id] integer NOT NULL PRIMARY KEY AUTOINCREMENT,
	[constraintCollectionId] integer NOT NULL,
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


CREATE TABLE [Scope]
(
	[targetId] integer NOT NULL,
	[constraintCollectionId] integer NOT NULL,
	FOREIGN KEY ([constraintCollectionId])
	REFERENCES [ConstraintCollection] ([id]),
	FOREIGN KEY ([targetId])
	REFERENCES [Target] ([id])
);


CREATE TABLE [TYPEE]
(
	[typee] text,
	[columnId] integer NOT NULL,
	[constraintId] integer NOT NULL,
	FOREIGN KEY ([constraintId])
	REFERENCES [Constraintt] ([id]),
	FOREIGN KEY ([columnId])
	REFERENCES [Columnn] ([id])
);



