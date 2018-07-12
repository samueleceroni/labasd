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
#define INSERT_INTO 0
#define SELECT 1
#define WHERE 2
#define ORDER_BY 3
#define GROUP_BY 4

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
		t = createTableDb(database, pRes->tableName, pRes->columns);
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

//Implementations
//TODO
void initDatabase(Database* db){
	(*db) = (Database) malloc (sizeof(struct DatabaseHead));
	(*db)->table = NULL;
	(*db)->next = NULL;
}

Table createTableDb(Database db, char* tableName, char** columns){
	//TODO
	return NULL;
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
	ParseResult * result = (ParseResult *) malloc (sizeof (ParseResult));

	if (!result) {
		return NULL;
	}

	result->success = false;

	char** queryType = {
		"CREATE TABLE",
		"INSERT INTO",
		"SELECT"
	};

	char* paramForbiddenChars = " ,.;*%$#@&^~\"'=+/\n\r!?()[]{}<>";

	int i=0, j=0, k=0;

	for (i=0; i<3; i++) { // iterate through queryType 
		for (j=0; queryType[i][j] && queryString[j]; j++) {
			// keep checking until one of the two is \0

			if (queryType[i][j] != queryString[j]) {
				// the chars don't match
				break;
			}			
		}

		if (queryType[i][j] == 0) {
			// if the selected queryType char is \0
			// it means the previous for stopped because it
			// reached the end of the queryType string
			// hence they are equal
			result->queryType = i-1;
			// i-1 works because of the defined values at the
			// beginning of this file
			break;
		}
	}

	// set i to the correct offset to continue parsing
	switch (result->queryType) {
		case CREATE_TABLE: // 12 char
			i=12;
			break;

		case INSERT_INTO: // 11 char
			i=11;
			break;

		case SELECT: // 6 char
			i=6;
			break;
	}

	// checking space between the command and the first parameter
	// EG: SELECT * .......
	//			 ^

	if (queryString[i] != ' ') {
		return result;	
	}

	// allocate first parameter string container
	char * param1 = (char *) malloc (sizeof(char *) * 1024);

	if (!param1) {
		return result;
	}

	// if the param char is allowed, append it to the string
	for (i=i+1, j=0; queryString[i]; i++, j++) {
		// SELECT query with * selector, skip the table name search directly
		if (j==0 && result->queryType == SELECT && queryString[i] == '*') {
			result->nColumns = -1;
			break;
		}

		if (isAllowed (queryString[i])) {
			param1[j] = queryString[i];
		} else {
			return result;
		}
	}

	// checking the space after the first param
	// EG: SELECT * FROM
	//             ^
	if (queryString[i] == ' ') {
		result->tableName = param1;
	} else { // is either the terminator or not allowed
		return result;
	}




	return result;
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
	//TODO
	return NULL;
}
