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

// Prototypes
Tree createTree(int key);
// Implementations
// TODO
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
