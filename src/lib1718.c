#include <lib1718.h>

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>

//Defines

//Query selectors
#define EQUAL 0
#define GREATER 1
#define LESSER 2
#define GREATER_EQUAL 3
#define LESSER_EQUAL 4

//Query types
#define CREATE_TABLE -1
#define WHERE 0
#define ORDER_BY 1
#define GROUP_BY 2
#define INSERT_INTO 3

//Flags di test
#ifdef TEST
	#define FOLDER "test/tables/"
#endif

#ifndef TEST
	#define FOLDER ""
#endif

static Database database = NULL;


//Main functions implematations
bool executeQuery(char* query){
	if(database == NULL)
		initDatabase(&database);
	ParseResult pRes = parseQuery(query);
	if(!pRes->success)
		return false;

	Table t = searchTableDb(database, pRes->tableName);
	if(t == NULL){
		t = loadTableFromFile(database, pRes->tableName);
	}

	if(pRes->queryType == CREATE_TABLE){
		if(t != NULL)
			return false;
		if(!createTableFile(pRes->tableName, pRes->columns))
			return false;
		t = createTableDb(database, pRes->tableName, pRes->columns, pRes->nColumns);
		generateLog(pRes);
	}
	else if(pRes->queryType == INSERT_INTO){
		if(t == NULL)
			return false;
		if(!insertIntoTable(t, createRecord(pRes->fieldValues)))
			return false;
		generateLog(pRes);
	}
	else
	{
		//SELECT
		QueryResultList selectResult;
		if(t == NULL)
			return false;

		selectResult = querySelect(t, pRes);
		generateLogSelect(pRes, selectResult);

		freeQueryResultList(selectResult);
	}
	freeParseResult(pRes);

	return true;
}


// Secondary functions prototypes

Tree createTree(int key);


// Implementations
void initDatabase(Database* db){
	(*db) = (Database) malloc (sizeof(struct DatabaseHead));
	(*db)->table = NULL;
	(*db)->next = NULL;
}

Table createTableDb(Database db, char* tableName, char** columns, int nColumns){
	// Case: Trying to create an existing table
	if (searchTableDb(db, tableName)){
		return NULL;
	}

	Table temp;
	// Try to allocate the table
	if (!(temp = (Table) malloc (sizeof(struct TableDB)))) {return NULL;}

	// Try to allocate the name of the table
	if(!(temp->name = (char*) malloc (strlen(tableName) * sizeof(char)))) {return NULL;}
	
	strcpy(temp->name, tableName);

	// Try to allocate the array of strings
	if(!(temp->columns = (char**) malloc (nColumns * sizeof(char*)))) {return NULL;}
	
	// Try to allocate each string and copy all of them 
	for (int i; i<nColumns; i++){
		if (!(temp->columns[i] = (char*) malloc(strlen(columns[i])*sizeof(char)))) {return NULL;}
		strcpy(temp->columns[i], columns[i]);
	}

	temp->nColumns = nColumns;
	temp->treeList = NULL;

	// Create and inserting all the trees. Start with i = nColumns so i can insert each tree in O(1) in the head
	for (int i=nColumns-1; i>=0; i--){
		// Create the Tree
		Tree newTree = createTree(i);
		// If createTree fails return error
		if (!newTree){return NULL;}
		// Insert the newTree in the head of the list of 
		newTree->next = temp->treeList;
		temp->treeList = newTree;
	}

	// the table is ready to be inserted into the database
	// Case database is empty
	if (!(db->table)){
		db->table=temp;
	}
	else{
	// try to create the newTable structure to be inserted as element of a list
	Database newTable = (Database)malloc(sizeof(struct DatabaseHead));
	if (!newTable) {return NULL;} // malloc fails

	newTable->table = temp;
	newTable->next = db->next;
	}

	return temp;

}

Table searchTableDb(Database db, char* tableName){
	//TODO
	return NULL;
}
NodeRecord createRecord(char** values){
	//TODO
	return NULL;
}
bool insertIntoTable(Table t, NodeRecord r){
	//TODO
	return false;
}

QueryResultList querySelect(Table t, ParseResult res){
	//TODO
	return false;
}
ParseResult parseQuery(char* queryString){
	//TODO
	return NULL;
}

void freeParseResult(ParseResult res){
	//TODO
	return;
}
void freeQueryResultList(QueryResultList res){
	//TODO
	return;
}

void generateLog(ParseResult res){
	//TODO
	return;
}
void generateLogSelect(ParseResult res, QueryResultList records){
	//TODO
	return;
}

bool checkTable(char* name){
	//TODO
	return false;
}
bool createTableFile(char* name, char** columns){
	//TODO
	return false;
}

Table loadTableFromFile(Database db, char* name){
	char* buffer = (char*)malloc(sizeof(char) * (strlen(name) + 20));
	strcpy(buffer, FOLDER);
	strcat(buffer, name);
	strcat(buffer, ".txt");

	#ifdef DEBUG
		printf("Trying to load table: %s ...\n", name);
	#endif

	FILE* f = fopen(buffer, "r");
	if(f == NULL){
	#ifdef DEBUG
		printf("Table not found!\nAborting...\n");
	#endif
		return NULL;
	}

	//Read table name and columns
	char header[] = {"TABLE "};
	char headerSeparator[] = {" COLUMNS "};
	char inlineSeparator = ',';
	char endlineSeparator = ';';

	//Check first header
	buffer = (char*)realloc(buffer, sizeof(char) * (strlen(header)+1));
	buffer = fgets(buffer, strlen(header)+1, f);
	buffer[strlen(header)] = '\0';

	if(strcmp(buffer, header) != 0){
	#ifdef DEBUG
		printf("Incorrect header! (%s)\nAborting...\n", buffer);
	#endif
		return NULL;
	}

	//Check table name
	buffer = (char*)realloc(buffer, sizeof(char) * (strlen(name)+1));
	buffer = fgets(buffer, strlen(name)+1, f);
	buffer[strlen(name)] = '\0';

	if(strcmp(buffer, name) != 0){
	#ifdef DEBUG
		printf("Incorrect table name! (%s)\nAborting...\n", buffer);
	#endif
		return NULL;
	}

	//Check header separator	
	buffer = (char*)realloc(buffer, sizeof(char) * (strlen(headerSeparator)+1));
	buffer = fgets(buffer, strlen(headerSeparator)+1, f);
	buffer[strlen(headerSeparator)] = '\0';

	if(strcmp(buffer, headerSeparator) != 0){
	#ifdef DEBUG
		printf("Incorrect header separator! (%s)\nAborting...\n", buffer);
	#endif
		return NULL;
	}

	//Reading fields
	char** columns = NULL;
	int nColumns = 0;

	char c = 0;
	do{
		c = fgetc(f);
		if(c == EOF){
	#ifdef DEBUG
			printf("Incorrect columns separation!\nAborting...\n");
	#endif
			return NULL;
		}
		nColumns++;
		columns = (char**)realloc(columns, sizeof(char*) * nColumns);
		char* column = NULL;
		int colLen = 0;
		while(c != inlineSeparator  && c != endlineSeparator){
			if(c == EOF){
	#ifdef DEBUG
				printf("Incorrect columns separation!\nAborting...\n");
	#endif
				return NULL;
			}
			colLen++;
			column = (char*)realloc(column, sizeof(char) * colLen);
			column[colLen-1] = c;
			c = fgetc(f);
		}
		if(colLen == 0){
	#ifdef DEBUG
			printf("Incorrect column size!\nAborting...\n");
	#endif
			return NULL;
		}
		//Insert terminal char
		column = (char*)realloc(column, sizeof(char) * colLen+1);
		column[colLen] = '\0';
		columns[nColumns-1] = column;
	}while(c != endlineSeparator);

	#ifdef DEBUG
	printf("Table: %s\n", name);
	printf("Columns:\n");
	for(int i = 0; i < nColumns; i++)
		printf("\t%s\n", columns[i]);
	#endif
	c = fgetc(f);

	Table t = createTableDb(db, name, columns, nColumns);
	if(t == NULL){
	#ifdef DEBUG
		printf("Creation gone wrong!\nAborting...\n");
	#endif
		return NULL;
	}

	//Reading rows
	//TODO
	char rowHeader[] = {"ROW "};
	
	char** row = NULL;

	do{
		if(c != '\n'){
	#ifdef DEBUG
			printf("Incorrect return space!\nAborting...\n");
	#endif
			return NULL;
		}

		//Check row header
		buffer = (char*)realloc(buffer, sizeof(char) * (strlen(rowHeader)+1));
		fgets(buffer, strlen(rowHeader)+1, f);
		buffer[strlen(rowHeader)] = '\0';
		if(strcmp(buffer, rowHeader) != 0){
	#ifdef DEBUG
			printf("Incorrect row header! (%s)\nAborting...\n", buffer);
	#endif
			return NULL;
		}

		row = (char**)malloc(sizeof(char*) * nColumns);

		//Reding row
		for(int i = 0; i < nColumns; i++){
			char* value = NULL;
			int size = 0;
			c = fgetc(f);
			if(c == EOF){
	#ifdef DEBUG
				printf("Incorrect return space!\nAborting...\n");
	#endif
				return NULL;
			}
			while(c != inlineSeparator && c != endlineSeparator){
				if(c == EOF){
	#ifdef DEBUG
					printf("Incorrect return space!\nAborting...\n");
	#endif
					return NULL;
				}
				size++;
				value = (char*)realloc(value, sizeof(char) * size);
				value[size-1] = c;
				c = fgetc(f);
			}
			value = (char*)realloc(value, sizeof(char) * (size+1));
			value[size] = '\0';
			if(c == endlineSeparator){
				if(nColumns - 1 != i){
	#ifdef DEBUG
					printf("Too few fields! (%d instead of %d)\nAborting...\n", i+1, nColumns);
	#endif
					return NULL;
				}
			}
			row[i] = value;
		}

		if(c != endlineSeparator){
			return NULL;
		}

	#ifdef DEBUG
		printf("Inserting row into table... (");
		for(int i = 0; i < nColumns; i++)
			printf("%s,", row[i]);
		printf("\b)\n");
	#endif
		if(!insertIntoTable(t, createRecord(row))){
	#ifdef DEBUG
			printf("Insertion gone wrong!\nAborting...\n");
			return NULL;
	#endif
		}

		c = fgetc(f);
	}while(c != EOF);
	
	#ifdef DEBUG
	printf("Table loaded!\n");
	#endif

	fclose(f);
	return t;
}

// Secondary functions implementation

Tree createTree(int key){
	
	Tree newTree = (Tree) malloc (sizeof(struct RBTree));
	
	if (!newTree){return NULL;} // CASE MALLOC FAILS
	newTree -> key = key;
	newTree -> root = NULL;
	newTree -> next = NULL; 
}
