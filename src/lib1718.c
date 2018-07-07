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

static Database db;

//Main functions implematations
bool executeQuery(char* query){
	ParseResult pRes = parseQuery(query);
	if(!pRes->success)
		return false;

	Table t = searchTableDb(db, pRes->tableName);
	if(t == NULL){
		t = loadTableFromFile(pRes->tableName);
		if(t != NULL)
			insertTableDb(db, t);
	}

	if(pRes->queryType == CREATE_TABLE){
		if(t != NULL)
			return false;
		if(!createTableFile(pRes->tableName, pRes->columns))
			return false;
		t = createTableDb(db, pRes->tableName, pRes->columns);
		generateLog(pRes);
	}
	else if(pRes->queryType == INSERT_INTO){
		if(t == NULL)
			return false;
		insertIntoTable(t, createRecord(pRes->fieldValues));
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
