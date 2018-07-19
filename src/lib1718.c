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
#define SELECT_WITHOUT_FILTERS 4
#define SELECT 5
#define NOT_VALID 6


// Query order
#define ASC 0
#define DESC 1

// For Parse
#define DECIMAL_SEPARATOR '.'

// Red Black Tree colors
#define RED 1
#define BLACK 0

// Red Black Tree node type
#define TABLE 0
#define RECORD 1

//Files
#define LOG_FILE_NAME "query_results.txt"

// Flags di test
#ifdef TEST
	#define FOLDER "test/"
	#define LOG_FOLDER "test/log/"
	#define TABLE_FOLDER "test/tables/"
#endif

#ifndef TEST
	#define FOLDER ""
	#define LOG_FOLDER ""
	#define TABLE_FOLDER ""
#endif

static Database database = NULL;


// Secondary functions prototypes
bool insertRecordTree(Tree T, Node z);
bool rbtInsertFixup(Tree T, Node z);
Node createNodeRBT(NodeRecord r);
bool leftRotate(Tree T, Node x) ;
bool rightRotate(Tree T, Node x);
int searchColumnIndex(Table T, char* key);//todo
void selectOrderBy(Node T, QueryResultList* queryToGet, int order);
void countForGroupBy(int key, QueryResultList queryToGet);
void selectWhere(NodeRecord r, QueryResultList* queryToGet, int keyIndex, int querySelector, char* keyName);

bool charIsAllowed (char c, const char * forbiddenCharSet);
double parseDouble (char * s);
int compare (char * a, char * b);
int strCompare (char * a, char * b);
int strIsNumber (char * s);
int strAreBothNumbers (char * a, char * b);

int fpeek(FILE * const fp);

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
	}
	else if(pRes->queryType == INSERT_INTO){
		if(t == NULL)
			return false;
		if(!insertRecordDb(t, createRecord(pRes->fieldValues, pRes->nColumns)))
			return false;
	}
	else
	{
		//SELECT
		QueryResultList selectResult;
		if(t == NULL)
			return false;

		selectResult = querySelect(t, pRes);
		generateLog(pRes, query, selectResult, database);

		freeQueryResultList(selectResult);
	}
	freeParseResult(pRes);

	return true;
}

void initDatabase(Database* db){
	// Trying to allocate the database structure
	(*db) = (Database) malloc (sizeof(struct DatabaseHead));
	// Initializing all the value to NULL
	(*db)->table = NULL;
	(*db)->next = NULL;
}

Table createTableDb(Database db, char* tableName, char** columns, int nColumns){ // creates the table and insert it into the DB
	// Case when you try to create an existing table
	if (searchTableDb(db, tableName)){
		return NULL;
	}
	// Case table is not found in database
	Table temp;
	// Try to allocate the table
	if (!(temp = (Table) malloc (sizeof(struct TableDB)))) {return NULL;}

	// Try to allocate the name of the table
	if(!(temp->name = (char*) malloc ((strlen(tableName)+1) * sizeof(char)))) {return NULL;}
	
	strcpy(temp->name, tableName);

	// save the number of columns of the table
	temp->nColumns = nColumns;
	
	// Try to allocate the array of strings
	if(!(temp->columns = (char**) malloc (nColumns * sizeof(char*)))) {return NULL;}
	
	// Try to allocate each string and copy all of them 
	for (int i=0; i<nColumns; i++){
		if (!(temp->columns[i] = (char*) malloc(strlen(columns[i])*sizeof(char)))) {return NULL;}
		strcpy(temp->columns[i], columns[i]);
	}
	
	// Allocate the  array of head of the trees
	if(!(temp->treeList = (Tree) malloc (nColumns*sizeof(struct RBTree)))){return NULL;}
	
	// Initialization of the trees
	for (int i=0; i<nColumns; i++){
		// Create the Tree
		temp->treeList[i].key = i;
		temp->treeList[i].root = NULL;
	}

	// the table is ready to be inserted into the database
	// Case database is empty
	if (!(db->table)){
		db->table = temp;
	}
	else{
	// try to create the newTable structure to be inserted as element of a list
	Database newTableHead = (Database)malloc(sizeof(struct DatabaseHead));
	if (!newTableHead) {return NULL;} // malloc fails

	newTableHead->table = temp;
	newTableHead->next = db->next;
	db->next = newTableHead;
	}
	
	return temp;
} //OK

Table searchTableDb(Database db, char* tableName){
	if (!db || !(db->table)) {return NULL;}	// Db is empty or the end of the queue is reached
	if (compare(db->table->name, tableName)==EQUAL){return db->table;}	// the table is found
	return searchTableDb(db->next, tableName);	// recursevely scroll the list
} //OK

NodeRecord createRecord(char** values, int nColumns){
	NodeRecord newRecord;
	// Try to allocate the new record structure and if fails returns NULL
	if (!(newRecord = (NodeRecord) malloc (sizeof(struct Record)))){return NULL;} // MALLOC FAILS
	newRecord->next = NULL;
	// Try to allocate values pointer and if fails returns NULL
	if (!(newRecord->values = (char**) malloc (nColumns*sizeof(char*)))) {return NULL;}
	// Try to allocate each values and if fails returns NULL
	for (int i=0; i<nColumns; i++){
		if (!(newRecord->values[i] = (char*) malloc(strlen(values[i])*sizeof(char)))) {return NULL;}
		strcpy(newRecord->values[i], values[i]);
	}
	return newRecord;	
} //OK

bool insertRecordDb(Table t, NodeRecord r){
	// table or record are not initilized, impossible to insert the record
	if(!t || !r){return false;}
	
	// insert the record into the head of the list of record
	r->next = t->recordList;
	t->recordList = r;

	// insert the element in each tree
	for(int i=0; i < t->nColumns; i++){
		if(!(insertRecordTree(&(t->treeList[i]), createNodeRBT(r)))){return false;}
	}
	return true; 
} //OK 

QueryResultList querySelect(Table t, ParseResult res){
	QueryResultList* queryToGet;
	if (!(queryToGet = (QueryResultList*) malloc(sizeof(QueryResultList)))){return NULL;}
	*queryToGet = NULL;

	int keyIndex = searchColumnIndex(t, res->key);
	if(keyIndex == -1){return NULL;}

	switch(res->queryType){
	case (ORDER_BY):
		selectOrderBy(t->treeList[keyIndex].root, queryToGet, res->order);
		break;
	case (GROUP_BY):
		selectOrderBy(t->treeList[keyIndex].root, queryToGet, res->order);
		countForGroupBy(keyIndex, (*queryToGet));
		break;
	case(WHERE):
		selectWhere(t->recordList, queryToGet, keyIndex, res->querySelector, res->keyName);
		break;
	default:
		break;
	}
	return *queryToGet;
} // OK


void unsuccessfulParse (ParseResult result) {
	result->success = false;
}

bool charIsAllowed (char c, const char * forbiddenCharSet) {
	int i;

	for (i=0; forbiddenCharSet[i]; i++) {
		if (c == forbiddenCharSet[i]) {
			return false;
		}
	}

	return true;
}



int parseQueryParameter (char * query, char ** parameter, const char * forbiddenCharSet) {
	const int paramSize = 1024;

	*parameter = (char *) malloc (sizeof(char) * paramSize);

	if (!*parameter) return 0;

	int i=0;

	for (i=0; query[i] && i<paramSize-1; i++) {
		if (charIsAllowed (query[i], forbiddenCharSet)) {
			// add it to the parsed parameter
			(*parameter)[i] = query[i];
		} else {
			// found not-allowed char
			return i;
		}
	}


	return i;
}



int parseQueryType (char * query) {
	const char * queryType[] = {
		"CREATE TABLE",
		"INSERT INTO",
		"SELECT"
	};

	int queryDefinedValues[] = {
		CREATE_TABLE,
		INSERT_INTO,
		SELECT
	};

	int i=0, j=0;

	for (i=0; i<3; i++) { // query first, so a not-matching query can be skipped @ the first char
		for (j=0; query[j] && queryType[i][j]; j++) {	

			if (query[j] != queryType[i][j]) {
				break;
			}
		}

		if (queryType[i][j] == 0) {
			return queryDefinedValues[i];
		}
	}

	return NOT_VALID;
}



void parseQueryCreateTable (char * query, ParseResult result) {
	const char * paramForbiddenChars = " ,.;*%$#@&^~\"'=+/\n\r!?()[]{}<>";
	const char space = ' ';
	const char comma = ',';

	// CREATE TABLE name
	//             ^ position 12
	query += 12; 

	// checking the space
	if (*query != ' ') {		
		unsuccessfulParse (result);
		return;
	}

	query++; // first char of tableName

	// parsing table name and shifting forward the pointer 
	query += parseQueryParameter (query, &(result->tableName), paramForbiddenChars);

	if (!result->tableName) {
		unsuccessfulParse (result);
		return;
	}

	// checking the space after table name
	if (*query != ' ') {
		unsuccessfulParse (result);	
		return;
	}

	query++;	// moving the pointer to the open bracket

	if (*query != '(') {
		unsuccessfulParse (result);
		return;
	}

	query++; // moving to the first column name char

	result->nColumns = 128;

	result->columns = (char **) malloc (result->nColumns * sizeof (char *));

	if (!result->columns) {
		unsuccessfulParse (result);
		return;
	}

	int i=0;
	for (i=0; true; i++) {

		if (i > result->nColumns) {
			char ** newColumns = (char **) realloc (result->columns, result->nColumns*2);

			if (!newColumns) {
				unsuccessfulParse(result);
				return;
			}

			result->nColumns *= 2;
			result->columns = newColumns;
		}

		query += parseQueryParameter (query, &(result->columns[i]), paramForbiddenChars);

		if (*query == ',') {
			query++;
			continue;
		}

		if (*query == ')') {
			i++;

			char ** reallocation = (char **) realloc (result->columns, i+1);

			if (!reallocation) {
				unsuccessfulParse (result);
				return;
			}

			result->columns = reallocation;
			result->nColumns = i;
			result->success = true;

		} else {
			unsuccessfulParse (result);
		}

		return;
	}
}



void parseQueryInsertInto (char * query, ParseResult result) {

	const char * paramForbiddenChars = " ,.;*%$#@&^~\"'=+/\n\r!?()[]{}<>";
	const char space = ' ';
	const char comma = ',';

	// INSERT INTO name
	//            ^ position 11
	query += 11; 

	// checking the space
	if (*query != ' ') {
		unsuccessfulParse(result);
		return;
	}

	query++; // first char of tableName

	// parsing table name and shifting forward the pointer 
	query += parseQueryParameter (query, &(result->tableName), paramForbiddenChars);

	if (!result->tableName) {
		unsuccessfulParse (result);
		return;
	}

	// checking the space after table name
	if (*query != ' ') {
		unsuccessfulParse (result);	
		return;
	}

	query++;	// moving the pointer to the open bracket

	if (*query != '(') {
		unsuccessfulParse (result);
		return;
	}

	query++; // moving to the first column name char

	// arbitrary number of columns
	result->nColumns = 128;

	// allocating memory for pointerz
	result->columns = (char **) malloc (result->nColumns * sizeof (char *));

	// checking pointer
	if (!result->columns) {
		unsuccessfulParse (result);
		return;
	}

	int i=0;

	// gettin' those column names
	for (i=0; true; i++) {

		// if there isn't enough space
		if (i > result->nColumns) {
			// double up!
			char ** newColumns = (char **) realloc (result->columns, result->nColumns*2);

			if (!newColumns) {
				unsuccessfulParse(result);
				return;
			}

			result->nColumns *= 2;
			result->columns = newColumns;
		}

		// now there is space for sure
		// and we parse the next parameter
		query += parseQueryParameter (query, &(result->columns[i]), paramForbiddenChars);
		// shifting the pointer @ the same time

		// comma = gotta read another column name
		if (*query == ',') {
			query++;
			continue;
		}

		// closed bracket, no more column names
		if (*query == ')') {
			i++;

			// now we can shrink the column list to fit, and save memory
			char ** reallocation = (char **) realloc (result->columns, i+1);

			if (!reallocation) {
				unsuccessfulParse (result);
				return;
			}

			result->columns = reallocation;
			result->nColumns = i;

		} else {
			unsuccessfulParse (result);
		}

		break;
	}

	query++;

	// check for " WHERE ("
	const char where[] = " VALUES (";

	for (i=0; i<9; i++) {
		if (query[i] != where[i]) {
			unsuccessfulParse (result);
			return;
		}
	}

	query += 9; // shift that pointer!

	// we expect N columns and N values
	// we already have nColumns so let's use it to init the array
	result->fieldValues = (char **) malloc (sizeof (char *) * result->nColumns);

	if (!result->fieldValues) {
		unsuccessfulParse(result);
		return;
	}

	for (i=0; i<result->nColumns; i++) {
		query += parseQueryParameter (query, &(result->fieldValues[i]), paramForbiddenChars);

		// comma = gotta read another value
		if (*query == ',') {
			query++;
			continue;
		}

		// closed bracket, no more values
		if (*query == ')') {

			result->success = true;
			return ;

		} else {
			unsuccessfulParse (result);
			return;
		}
	}
}


ParseResult parseQuery (char* queryString){
	ParseResult result = (ParseResult) malloc (sizeof (struct ParseResult));

	if (!result) {
		return NULL;
	}

	result->success = false;	

	char * paramForbiddenChars = " ,.;*%$#@&^~\"'=+/\n\r!?()[]{}<>";

	int queryType = parseQueryType (queryString);

	switch (queryType) {
		case CREATE_TABLE:
			parseQueryCreateTable (queryString, result);
			break;

		case INSERT_INTO:
			parseQueryInsertInto (queryString, result);
			break;

		case SELECT:
			// parseQuerySelect (queryString, result);
			break;

		default:
			result->success = false;
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

void generateLog(ParseResult pRes, char* query, QueryResultList records, Database db){
	char* buffer = (char*)malloc(sizeof(char) * (strlen(LOG_FILE_NAME) + strlen(FOLDER) + 1));
	strcpy(buffer, LOG_FOLDER);
	strcat(buffer, LOG_FILE_NAME);

	#ifdef DEBUG
	printf("Inserting log into %s ...\n", buffer);
	#endif

	FILE* f = fopen(buffer, "a+");

	if(f == NULL){
	#ifdef DEBUG
		printf("Error while creating/opening %s!\nAborting...", buffer);
	#endif
		return;
	}

	fprintf(f, "%s", query);
	if(query[strlen(query)-1] != '\n')
		fprintf(f, "\n");

	//Inserting header
	fprintf(f, "TABLE %s COLUMNS ", pRes->tableName);
	int i;
	if(pRes->queryType == GROUP_BY){
		fprintf(f, "%s,COUNT;\n", pRes->keyName);
	} else {
		for(i = 0; i < pRes->nColumns; i++){
			fprintf(f, "%s", pRes->columns[i]);
			if(i != pRes->nColumns - 1)
				fprintf(f, ",");
			else
				fprintf(f, ";\n");
		}
	}

	Table t = searchTableDb(db, pRes->tableName);
	if(t == NULL){
	#ifdef DEBUG
		printf("Table %s not found!\nAborting...\n", pRes->tableName);
	#endif
		return;
	}

	//Inserting records
	if(pRes->queryType == SELECT_WITHOUT_FILTERS){
		NodeRecord r = t->recordList;
		while(r != NULL){
			fprintf(f, "ROW ");
			for(i = 0; i < t->nColumns; i++){
				fprintf(f, "%s", r->values[i]);
				if(i != pRes->nColumns - 1)
					fprintf(f, ",");
				else
					fprintf(f, ";\n");	
			}
		}
		fclose(f);
		return;
	}

	while(records != NULL){
		fprintf(f, "ROW ");
		if(pRes->queryType == GROUP_BY){
			int colIndex = 0;
			colIndex = searchColumnIndex(t, pRes->keyName);
			if(colIndex == -1){return;}
			fprintf(f, "%s,%d;\n", records->nodeValue->values[colIndex], records->occurrence);
		} else {
			for(i = 0; i < pRes->nColumns; i++){
				int colIndex = i;
				colIndex = searchColumnIndex(t, pRes->columns[i]);
				if(colIndex == -1){return;}
				fprintf(f, "%s", records->nodeValue->values[colIndex]);
				if(i != pRes->nColumns - 1)
					fprintf(f, ",");
				else
					fprintf(f, ";\n");	
			}
		}
		records = records->next;
	}

	fclose(f);
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
	char* buffer = (char*)malloc(sizeof(char) * (strlen(name) + strlen(TABLE_FOLDER) + 5));
	strcpy(buffer, TABLE_FOLDER);
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

	Table t = NULL;
	
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
	
	t = createTableDb(db, name, columns, nColumns);
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
	#endif
			return NULL;
		}

		c = fgetc(f);

	}while(c != EOF && fpeek(f) != EOF);
		
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
	if( !z || !T ) return false;
	Node y = NULL;
	Node x = T->root;
	while(x!=NULL){
		y = x;
		if(compare(z->nodeValue->values[T->key], y->nodeValue->values[T->key])==LESSER){
			x = x->left;
		}
		else{
			x = x->right;
		}
	}
	z->p = y;
    if (y == NULL)
        T->root = z;
    else
        if (compare(z->nodeValue->values[T->key], y->nodeValue->values[T->key])==LESSER)
            y->left = z;
        else
            y->right = z;
	z->left = NULL;
	z->right = NULL;
	z->color = RED;

	return rbtInsertFixup(T, z);
}

bool rbtInsertFixup(Tree T, Node z){
	if( !T || !z ){return false;}

	Node y;

	while (z->p && z->p->color == RED) {
	    if (z->p == z->p->p->left) {
	        y = z->p->p->right;
	        if (y && y->color == RED) {
	            z->p->color = BLACK;
	            y->color = BLACK;
	            z->p->p->color = RED;
	            z = z->p->p;
	        }
	        else {
	            if (z == z->p->right) {
	                z = z->p;
	                if(!(leftRotate(T, z))){return false;}
	            }
	            z->p->color = BLACK;
	            z->p->p->color = RED;
	            if(!(rightRotate(T, z->p->p))){return false;}
	        }
	    }
	    else {
	        y = z->p->p->left;
	        if (y && y->color == RED) {
	            z->p->color = BLACK;
	            y->color = BLACK;
	            z->p->p->color = RED;
	            z = z->p->p;
	        }
	        else {
	            if (z == z->p->left) {
	                z = z->p;
	                if(!(rightRotate(T, z))){return false;}
	            }
	            z->p->color = BLACK;
	            z->p->p->color = RED;
	            if(!(leftRotate(T, z->p->p))){return false;}
	        }
	    }
	}
	T->root->color = BLACK;
	return true;
}

bool leftRotate(Tree T, Node x) {
    if( !T || !x ){return false;}
    Node y = x->right;
    x->right = y->left;
    if (y->left != NULL)
        y->left->p = x;
    y->p = x->p;
    if (x->p == NULL)
        T->root = y;
    else
        if (x == x->p->left)
            x->p->left = y;
        else
            x->p->right = y;
    y->left = x;
    x->p = y;
    return true;
}

bool rightRotate(Tree T, Node x) {
    if( !T || !x ){return false;}

    Node y = x->left;
    x->left = y->right;
    if (y->right != NULL)
        y->right->p = x;
    y->p = x->p;
    if (x->p == NULL)
        T->root = y;
    else
        // x was the left son of his father
        if (x == x->p->left)
            x->p->left = y;
    // x was the right son of his father
        else
            x->p->right = y;
    y->right = x;
    x->p = y;
    return true;
}

void selectOrderBy(Node x, QueryResultList* queryToGet, int order){
	if(!x){return;}
	if(order!=ASC && order!=DESC){return;}

	// I have to walk into the tree in the opposite way because i push elements to the head of the list
	if(order==ASC){
		selectOrderBy(x->right, queryToGet, order);
	}
	else{
		selectOrderBy(x->left, queryToGet, order);
	}

	QueryResultList newElement;
	if (!(newElement = (QueryResultList) malloc (sizeof(struct QueryResultElement)))){return;}
	newElement->next = (*queryToGet);
	newElement->nodeValue = x->nodeValue;
	newElement->occurrence = 1;
	*queryToGet = newElement;

	if(order==ASC){
		selectOrderBy(x->left, queryToGet, order);
	}
	else{
		selectOrderBy(x->right, queryToGet, order);
	}
}

void countForGroupBy(int key, QueryResultList queryToGet){
	QueryResultList temp = queryToGet, temp2;
	while(temp && temp->next){
		if (compare(temp->nodeValue->values[key],temp->next->nodeValue->values[key])==EQUAL){
			temp->occurrence++;
			temp2 = temp->next;
			temp->next = temp2->next;
			free(temp2);
		}
		else{
			temp = temp->next;
		}
	}
}

void selectWhere(NodeRecord r, QueryResultList* queryToGet, int keyIndex, int querySelector, char* key){
	if( !r || keyIndex < 0 || querySelector < 0 || querySelector > 4 || !key ){return;}
	
	int comparison = compare(r->values[keyIndex], key);
	bool addToList = false;
	switch(querySelector){
		case(GREATER):
			if(comparison == GREATER){addToList=true;}
			break;
		
		case(GREATER_EQUAL):
			if(comparison == GREATER || comparison == EQUAL){addToList=true;}
			break;
		
		case(EQUAL):
			if(comparison == EQUAL){addToList=true;}
			break;
		
		case(LESSER_EQUAL):
			if(comparison == LESSER || comparison == EQUAL){addToList=true;}
			break;
		
		case(LESSER):
			if(comparison == LESSER){addToList=true;}
			break;
		default:
			break;
	}
	if(addToList){
		QueryResultList newElement;
		if(!(newElement = (QueryResultList) malloc(sizeof(struct QueryResultElement)))){return;}
		newElement->next = (*queryToGet);
		*queryToGet = newElement;
		newElement->occurrence=1;
		newElement->nodeValue = r;
	}
	selectWhere(r->next, queryToGet, keyIndex, querySelector, key);
	return;
}

int searchColumnIndex(Table T, char* key){
	int i = 0;
	while(i < T->nColumns){
		if(compare(key, T->columns[i]) == EQUAL){return i;}
		i++;
	}
	return -1;
}

int fpeek(FILE * const fp)
{
  const int c = getc(fp);
  return c == EOF ? EOF : ungetc(c, fp);
}
