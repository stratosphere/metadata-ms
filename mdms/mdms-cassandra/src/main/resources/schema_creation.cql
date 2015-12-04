		CREATE TABLE schemaa (
				id int PRIMARY KEY,
				name text,
				schema_path text,
				description text,
				type text);
				
		CREATE TABLE tablee (
				id int PRIMARY KEY,
				schema_id int,
				location_properties map<text, text>,
				description text,
				type text);
		
		CREATE TABLE columnn (
				id int PRIMARY KEY,
				table_id int,
				indexx int,
				name text,
				description text,
				type text);
		
		CREATE TABLE constraint_collectionn (
				id int PRIMARY KEY,
				schema_id int,
				description text);
		
		CREATE TABLE constraintt (
				id uuid PRIMARY KEY,
				type text,
				constraint_collection_id int,
				lhs_FD set<int>,
				rhs_FD int,
				pair_IND map<int, int>,
				UCC set<int>,
				countt text,
				count_columnID text);
	
		CREATE INDEX constraint_t
				 ON constraintt (type);