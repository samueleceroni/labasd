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
	char* buffer = (char*)malloc(sizeof(char) * (strlen(name) + 20));
	strcpy(buffer, FOLDER);
	strcat(buffer, name);
	strcat(buffer, ".txt");

	FILE* f = fopen(buffer, "r");
	if(f == NULL)
		return NULL;

	//Read table name and columns
	char header[] = {"TABLE "};
	char headerSeparator[] = {" COLUMNS "};
	char inlineSeparator = ',';
	char endlineSeparator = ';';

	//Check first header
	buffer = (char*)realloc(buffer, sizeof(char) * (strlen(header)+1));
	buffer = fgets(buffer, strlen(header), f);
	buffer[strlen(header)] = '\0';

	if(strcmp(buffer, header) != 0)
		return NULL;

	//Check table name
	buffer = (char*)realloc(buffer, sizeof(char) * (strlen(name)+1));
	buffer = fgets(buffer, strlen(name), f);
	buffer[strlen(name)] = '\0';

	if(strcmp(buffer, name) != 0)
		return NULL;

	//Check header separator	
	buffer = (char*)realloc(buffer, sizeof(char) * (strlen(headerSeparator)+1));
	buffer = fgets(buffer, strlen(headerSeparator), f);
	buffer[strlen(headerSeparator)] = '\0';

	if(strcmp(buffer, headerSeparator) != 0)
		return NULL;

	//Reading fields
	char** columns;
	int nColumns = 0;

	char c = 0;
	while((c != endlineSeparator)){
		nColumns++;
		columns = (char**)realloc(columns, sizeof(char*) * nColumns);
		char* column;
		int colLen = 0;
		while(c != inlineSeparator  && c != endlineSeparator){
			colLen++;
			column = (char*)realloc(column, sizeof(char) * colLen);
			column[colLen-1] = c;
			c = fgetc(f);
		}
		//Insert terminal char
		column = (char*)realloc(column, sizeof(char) * colLen+1);
		column[colLen] = '\0';
		columns[nColumns-1] = column;
		c = fgetc(f);
	}

	//Reading rows
	//TODO
	return NULL;
}
