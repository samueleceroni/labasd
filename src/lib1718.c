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

//Main functions implematations
bool executeQuery(char* query){
	ParseResult* pRes = parseQuery(query);
	if(!pRes->success)
		return false;

	Table* t = searchTable(tables, pRes->tableName);
	if(t == NULL){
		t = loadTable(pRes->tableName);
		if(t != NULL)
			insertTable(tables, t);
	}

	if(pRes->queryType == CREATE_TABLE){
		if(t != NULL)
			return false;
		t = createTable(pRes->tableName, pRes->columns);
		if(t != NULL)
			insertTable(tables, t);
		else
			return false;
		generateLog(pRes);
	}
	else if(pRes->queryType == INSERT_INTO){
		if(t == NULL)
			return false;
		Record* r = createRecord(pRes->columns, pRes->fieldValues);
		insertIntoTable(t, r);
		generateLog(pRes);
	}
	else
	{
		//SELECT
		ListOfRecord* selectResult;
		bool success = true;

		if(t == NULL)
			return false;

		selectResult = querySelect(t, pRes);
		generateLogSelect(pRes, selectResult);

		freeListOfRecord(selectResult);
	}

	freeParseResult(pRes);

	return true;
}

//Implementations
//TODO
