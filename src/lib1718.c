#include <lib1718.h>

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <math.h>

// Defines

// Query selectors
#define EQUAL 0
#define GREATER 1
#define LESSER 2
#define GREATER_EQUAL 3
#define LESSER_EQUAL 4

// Query types
#define CREATE_TABLE -1
#define WHERE 0
#define ORDER_BY 1
#define GROUP_BY 2
#define INSERT_INTO 3

// For Parse
#define DECIMAL_SEPARATOR '.'

// Flags di test
#ifdef TEST
	#define FOLDER "test/tables/"
#endif

#ifndef TEST
	#define FOLDER ""
#endif

static Database database = NULL;


// Secondary functions prototypes
void insertRecordTree(Tree T, Node z);
void rbtInsertFixup(Tree T, Node z);
Node createNodeRBT(NodeRecord r);

double parseDouble (char * s);
int compare (char * a, char * b);
int strCompare (char * a, char * b);
int strIsNumber (char * s);
int strAreBothNumbers (char * a, char * b);

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
		if(!insertRecordDb(t, createRecord(pRes->fieldValues, pRes->nColumns)))
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

// Implementations
void initDatabase(Database* db){
	(*db) = (Database) malloc (sizeof(struct DatabaseHead));
	(*db)->table = NULL;
	(*db)->next = NULL;
}

Table createTableDb(Database db, char* tableName, char** columns, int nColumns){ // creates the table and insert it into the DB
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
	// Allocate the head of all the trees
	if(!(temp->treeList = (Tree) malloc (nColumns*sizeof(struct RBTree)))){return NULL;}

	// Create and inserting all the trees. Start with i = nColumns so i can insert each tree in O(1) in the head
	for (int i=0; i<nColumns; i++){
		// Create the Tree
		temp->treeList[i].key = i;
		temp->treeList[i].root = NULL;
	}

	// the table is ready to be inserted into the database
	// Case database is empty
	if (!(db->table)){
		db->table=temp;
	}
	else{
	// try to create the newTable structure to be inserted as element of a list
	Database newTableHead = (Database)malloc(sizeof(struct DatabaseHead));
	if (!newTableHead) {return NULL;} // malloc fails

	newTableHead->table = temp;
	newTableHead->next = db->next;
	}

	return temp;
} //OK, NOT FINALLY TESTED

Table searchTableDb(Database db, char* tableName){
	if (!db) {return NULL;}	// Db is empty or the end of the queue is reached
	if (compare(db->table->name, tableName)==EQUAL){return db->table;}	// the table is found
	return searchTableDb(db->next, tableName);	// recursevely return the searchTable on the next table
} //OK, NOT FINALLY TESTED

NodeRecord createRecord(char** values, int nColumns){
	NodeRecord newRecord = (NodeRecord) malloc (sizeof(struct Record));
	if (!(newRecord)){return NULL;} // MALLOC FAILS
	newRecord->next = NULL;
	if (!(newRecord->values = (char**) malloc (nColumns*sizeof(char*)))) {return NULL;}
	for (int i; i<nColumns; i++){
		if (!(newRecord->values[i] = (char*) malloc(strlen(values[i])*sizeof(char)))) {return NULL;}
		strcpy(newRecord->values[i], values[i]);
	}
	return newRecord;	
} //OK, NOT FINALLY TESTED

bool insertRecordDb(Table t, NodeRecord r){
	if(!t || !r){return false;} // table or record are not initilized, impossible to insert the record
	
	// insert the record into the head of the list of record
	r->next = t->recordList;
	t->recordList = r;

	// insert the element in each tree
	for(int i=0; i < t->nColumns; i++){
		if(!(insertRecordTree(&(t->treeList[i]), createNodeRBT(r)))){return false;}
	}
	return true; 
} //OK, NOT FINALLY TESTED 

QueryResultList querySelect(Table t, ParseResult res){
	//TODO
	return false;
} //TODO

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
		if(!insertRecordDb(t, createRecord(row, nColumns))){
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
int strCompare (char * a, char * b) {	
	int res = strcmp (a, b);

	if (res < 0) {
		return LESSER;
	}

	if (res > 0) {
		return GREATER;
	}

	return EQUAL;
}

int strIsNumber (char * s) {
	int size = 0;
	int i = 0;
	int isNumber = true;

	for (i=0; s[i]; i++, size++) {
		if (s[0] == '-') {
			continue;
		}

		if ((s[i] < '0' || s[i] > '9') && s[i] != DECIMAL_SEPARATOR) {
			isNumber = false;
			break;
		}
	}	

	return isNumber;
}

int strAreBothNumbers (char * a, char * b) {
	// useless comment	
	return strIsNumber(a) && strIsNumber(b);
}

double parseDouble (char * s) {
	int i = 0;
	int separatorFound = false;
	int exponent = -1;
	int iStart = 0;

	double signMultiplier = 1.0;
	double result = 0;

	if (s[0] == '-') {	// if negative
		signMultiplier = -1.0;	// at the end, multiply by -1
		iStart = 1;	// start the conversion from 1 (0 is '-')
	}

	// increment by one the exponent 'till the separator or terminator is found
	for (i=iStart; s[i]!='.' && s[i]; i++, exponent++) {}

	// convert the number
	for (i=iStart; s[i]; i++, exponent--){
		if (s[i] == '.') { // you want to skip the '.'
			exponent++;	// and compensate the exponent decrementation
			continue;
		}

		// char to int times the correct ten power, casted as double
		result += (double) ((s[i] - '0') * pow(10, exponent));
	}

	return result * signMultiplier;
}

int compare (char * a, char * b) {	// compares two strings
	double numA = 0, numB = 0;

	if (strAreBothNumbers (a, b)) {
		numA = parseDouble(a);
		numB = parseDouble(b);

		if (numA > numB) {
			return GREATER;
		}

		if (numA < numB) {
			return LESSER;
		}

		return EQUAL;
	}

	return strCompare (a, b);
}

Node createNodeRBT(NodeRecord r){
	Node x;
	if(!(x = (Node) malloc(sizeof(struct RBTNode)))){return NULL;}
	x->nodeValue = r;
	return x;
}
bool insertRecordTree(Tree T, Node z){

	Node y = NULL;
	Node x = T->root;
	while(x!=NULL){
		y = x;
		if(/*.............*/){
			x = x->left
		}
	}
}
