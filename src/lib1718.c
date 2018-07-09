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
	//TODO
	return NULL;
}
