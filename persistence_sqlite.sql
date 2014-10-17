

/* Create Tables */

CREATE TABLE [Target]
(
	[id] integer NOT NULL,
	PRIMARY KEY ([id])
);


CREATE TABLE [Schemaa]
(
	[name] text,
	[id] integer NOT NULL,
	[location] text,
	PRIMARY KEY ([id]),
	FOREIGN KEY ([id])
	REFERENCES [Target] ([id])
);


CREATE TABLE [Tablee]
(
	[name] text,
	[id] integer NOT NULL,
	[schemaId] integer NOT NULL,
	[location] text,
	PRIMARY KEY ([id]),
	FOREIGN KEY ([id])
	REFERENCES [Target] ([id]),
	FOREIGN KEY ([schemaId])
	REFERENCES [Schemaa] ([id])
);


CREATE TABLE [Columnn]
(
	[name] text,
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
	FOREIGN KEY ([rhs])
	REFERENCES [Columnn] ([id]),
	FOREIGN KEY ([lhs])
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
	FOREIGN KEY ([columnId])
	REFERENCES [Columnn] ([id]),
	FOREIGN KEY ([constraintId])
	REFERENCES [Constraintt] ([id])
);



