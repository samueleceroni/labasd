#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <math.h>

#include "lib1718.h"

// Defines

// Query selectors
#define EQUAL 0
#define GREATER 1
#define LESSER 2
#define GREATER_EQUAL 3
#define LESSER_EQUAL 4
#define NO_OPERATOR 5

// Query types
#define CREATE_TABLE -1
#define WHERE 0
#define ORDER_BY 1
#define GROUP_BY 2
#define INSERT_INTO 3
#define SELECT_WITHOUT_FILTERS 4
#define SELECT 5
#define NO_QUERY 6


// Query order
#define ASC 0
#define DESC 1

// For Parse
#define DECIMAL_SEPARATOR '.'

// Red Black Tree colors
#define RED 1
#define BLACK 0

// Red Black Tree key type
#define TABLE -2

//Files
#define LOG_FILE_NAME "query_results.txt"

//Memory usage max threshold
#define MEMORY_THRESHOLD 256000000

static Database database = NULL;
static TableHeap memoryHeap = NULL;
static unsigned long long int priorityCounter = 1;
static Table currentTableUsed = NULL;


// Secondary functions prototypes
bool insertNodeTree(Tree T, Node z);
bool rbtInsertFixup(Tree T, Node z);
Node createNodeRBT(void * r);
void treeTransplant(Tree T, Node u, Node v);
void removeNodeRBT(Tree T, Node z);
void rbtDeleteFixup(Tree T, Node x);
Node treeMinimum(Node x);
bool leftRotate(Tree T, Node x);
bool rightRotate(Tree T, Node x);
int searchColumnIndex(Table T, char* key);
void selectOrderBy(Node T, QueryResultList* queryToGet, int order);
void countForGroupBy(int key, QueryResultList queryToGet);
void selectWhere(NodeRecord r, QueryResultList* queryToGet, int keyIndex, int querySelector, char* keyName);
void selectNoFilter(NodeRecord r, QueryResultList* queryToGet);
Node searchNodeTableDb(Node currentTableNode, char* tableName);
void deleteAllTreeRecordNodes(Node x);
void deleteAllRecords(NodeRecord n, int nColumns);

bool charIsAllowed(char c, const char * forbiddenCharSet);
ParseResult parseQuerySelect(char * query, ParseResult result);
ParseResult parseQueryCreateTable(char * query, ParseResult result);
ParseResult parseQueryInsertInto(char * query, ParseResult result);
ParseResult parseQuerySelectWHERE(char * query, ParseResult result);
ParseResult parseQuerySelectORDERBY(char * query, ParseResult result);
ParseResult parseQuerySelectGROUPBY(char * query, ParseResult result);


double parseDouble(char * s);
int compare(char * a, char * b);
int strCompare(char * a, char * b);
int strIsNumber(char * s);
int strAreBothNumbers(char * a, char * b);

int fpeek(FILE * const fp);

//Memory management secondary functions
void* allocateBytes(int bytes);
bool moreThanOneTableAllocated();
int deallocateFurthestTable();

void bubbleUpHeapElement(TableHeapElement el);
void bubbleDownHeapElement(TableHeapElement el);
void swapHeapElement(int a, int b);

//Main functions implementations
bool executeQuery(char* query) {
	if (database == NULL) {
		initDatabase(&database);
	}
	if (memoryHeap == NULL) {
		initMemoryHeap();
	}
	ParseResult pRes = parseQuery(query);
	if (!pRes->success) {
		return false;
	}

	Table t = searchTableDb(database, pRes->tableName);
	if (t == NULL) {
		t = loadTableFromFile(database, pRes->tableName);
	}
	else {
		updatePriorityMemoryHeap(t->heapReference, priorityCounter++);
		currentTableUsed = t;
	}

	if (t != NULL && !checkQueryIntegrity(t, pRes)) { return false; }

	if (pRes->queryType == CREATE_TABLE) {
		if (t != NULL) {
			return false;
		}
		if (!createTableFile(pRes->tableName, pRes->columns, pRes->nColumns)) {
			return false;
		}
		t = createTableDb(database, pRes->tableName, pRes->columns, pRes->nColumns);
		if (t == NULL) {
			return false;
		}
	}
	else if (pRes->queryType == INSERT_INTO) {
		if (t == NULL) {
			return false;
		}
		if (!insertIntoTableFile(pRes->tableName, pRes->columns, pRes->fieldValues, pRes->nColumns)) {
			return false;
		}

		if (!insertRecordDb(t, createRecord(pRes->fieldValues, pRes->nColumns))) {
			return false;
		}
	}
	else
	{
		//SELECT
		QueryResultList selectResult;
		if (t == NULL) {
			return false;
		}

		selectResult = querySelect(t, pRes);
		generateLog(pRes, query, selectResult, database);

		freeQueryResultList(selectResult);
	}
	freeParseResult(pRes);

	return true;
}

void initDatabase(Database* db) {
	// Trying to allocate the database structure
	(*db) = (Database)malloc(sizeof(struct RBTree));

	(*db)->key = TABLE;
	(*db)->root = NULL;
}

Table createTableDb(Database db, char* tableName, char** columns, int nColumns) {
	// creates the table and insert it into the DB
	// Case: Trying to create an existing table
	if (searchTableDb(db, tableName)) {
		return NULL;
	}
	// Case table is not found in database
	Table newTable;
	int i;
	// Try to allocate the table
	if (!(newTable = (Table)malloc(sizeof(struct TableDB)))) { return NULL; }
	//Inserting into memory management heap
	TableHeapElement el = insertMemoryHeap(newTable);
	newTable->heapReference = el;

	//Updating current table used
	currentTableUsed = newTable;
	// Try to allocate the name of the table

	if (!(newTable->name = (char*)allocateBytes((strlen(tableName) + 1) * sizeof(char)))) { return NULL; }

	strcpy(newTable->name, tableName);

	// save the number of columns of the table
	newTable->nColumns = nColumns;

	// Try to allocate the array of strings
	if (!(newTable->columns = (char**)allocateBytes(nColumns * sizeof(char*)))) { return NULL; }

	// Try to allocate each string and copy all of them
	for (i = 0; i<nColumns; i++) {
		if (!(newTable->columns[i] = (char*)allocateBytes(strlen(columns[i]) * sizeof(char)))) { return NULL; }
		strcpy(newTable->columns[i], columns[i]);
	}

	// Allocate the array of head of the trees
	if (!(newTable->treeList = (Tree)allocateBytes(nColumns * sizeof(struct RBTree)))) { return NULL; }

	// Initialization of the trees

	for (i = 0; i<nColumns; i++) {
		// Create the Tree
		newTable->treeList[i].key = i;
		newTable->treeList[i].root = NULL;
	}

	Node newTableNode = createNodeRBT((void*)newTable);
	if (insertNodeTree(db, newTableNode) == false) { return NULL; }
	newTable->recordList = NULL;
	return newTable;
}

Table searchTableDb(Database db, char* tableName) {
	Node currentTableNode = searchNodeTableDb(db->root, tableName);
	if (currentTableNode == NULL || currentTableNode->nodeValue == NULL) {
		return NULL;
	}
	return (Table)currentTableNode->nodeValue;
}

Node searchNodeTableDb(Node currentTableNode, char* tableName) {
	if (currentTableNode == NULL || currentTableNode->nodeValue == NULL) {
		return NULL;
	}
	Table currentTable = (Table)currentTableNode->nodeValue;

	switch (compare(tableName, currentTable->name)) {
	case(EQUAL):
		return currentTableNode;
	case(LESSER):
		return searchNodeTableDb(currentTableNode->left, tableName);
	case(GREATER):
		return searchNodeTableDb(currentTableNode->right, tableName);
	default:
		return NULL;
	}
}

void deallocateTable(Database db, Table t) {
	if (!db || !t) { return; }
	Node nodeToBeDeallocated = searchNodeTableDb(db->root, t->name);
	if (nodeToBeDeallocated) {
		removeNodeRBT(db, nodeToBeDeallocated);
		// deallocate all the values in the table t
		int i;
		free(t->name);
		for (i = 0; i < t->nColumns; i++) {
			free(t->columns[i]);
			deleteAllTreeRecordNodes((t->treeList[i].root));
		}
		free(t->columns);
		deleteAllRecords(t->recordList, t->nColumns);
		free(t);
		free(nodeToBeDeallocated);
	}
}

void treeTransplant(Tree T, Node u, Node v) {
	if (!T || !u) { return; }

	if (!u->p) {
		T->root = v;
	}
	else if (u == u->p->left) {
		u->p->left = v;
	}
	else {
		u->p->right = v;
	}
	if (v) {
		v->p = u->p;
	}
}

void removeNodeRBT(Tree T, Node z) {
	if (!T || !z) {
		return;
	}
	Node x, y = z;
	bool yOriginalColor = y->color;
	if (!z->left) {
		x = z->right;
		treeTransplant(T, z, z->right);
	}
	else if (!z->right) {
		x = z->left;
		treeTransplant(T, z, z->left);
	}
	else {
		y = treeMinimum(z->right);
		yOriginalColor = y->color;
		x = y->right;
		if (x && y->p == z) {
			x->p = y;
		}
		else {
			treeTransplant(T, y, y->right);
			y->right = z->right;
			if (y->right) {
				y->right->p = y;
			}
		}
		treeTransplant(T, z, y);
		y->left = z->left;
		y->left->p = y;
		y->color = z->color;
	}
	if (yOriginalColor == BLACK) {
		rbtDeleteFixup(T, x);
	}

} // TOCHECK

void rbtDeleteFixup(Tree T, Node x) {
	Node w;
	while (T && x && (x != T->root) && x->color == BLACK) {
		if (x == x->p->left) { // i don't check the existence of x->p because it's not the root
			w = x->p->right;
			if (w->color == RED) {
				w->color = BLACK;
				x->p->color = RED;
				leftRotate(T, x->p);
				w = x->p->right;
			}
			if (w->left && w->left->color == BLACK && w->right && w->right->color == BLACK) {
				w->color = RED;
				x = x->p;
			}
			else {
				if (w->right && w->left && w->right->color == BLACK) {
					w->left->color = BLACK;
					w->color = RED;
					rightRotate(T, w);
					w = x->p->right;
				}
				w->color = x->p->color;
				x->p->color = BLACK;
				if (w->right) {
					w->right->color = BLACK;
				}
				leftRotate(T, x->p);
				x = T->root;
			}
		}
		else {
			w = x->p->left;
			if (w && w->color == RED) {
				w->color = BLACK;
				x->p->color = RED;
				rightRotate(T, x->p);
				w = x->p->left;
			}
			if (w->left && w->left->color == BLACK && w->right && w->right->color == BLACK) {
				w->color = RED;
				x = x->p;
			}
			else {
				if (w->left && w->right && w->left->color == BLACK) {
					w->right->color = BLACK;
					w->color = RED;
					rightRotate(T, w);
					w = x->p->left;
				}
				w->color = x->p->color;
				x->p->color = BLACK;
				if (w->left) {
					w->left->color = BLACK;
				}
				rightRotate(T, x->p);
				x = T->root;
			}
		}
	}
	if (x) {
		x->color = BLACK;
	}
} // TOCHECK

Node treeMinimum(Node x) {
	while (x && x->left) {
		x = x->left;
	}
	return x;
}

void deleteAllTreeRecordNodes(Node x) {  // specifically for RecordNodes because the nodeValue in this
	if (!x) { return; }                   // case is not allocated but contains an address, while if you need
	deleteAllTreeRecordNodes(x->left);  // to deallocate all TableNodes you should deallocate the table itself
	deleteAllTreeRecordNodes(x->right);
	free(x);
}

void deleteAllRecords(NodeRecord n, int nColumns) {
	if (!n) { return; }
	deleteAllRecords(n->next, nColumns);
	// deallocate the content of each Record
	int i;
	for (i = 0; i<nColumns; i++) {
		free(n->values[i]);
	}
	free(n->values);

	// deallocate the Record itself
	free(n);
}

NodeRecord createRecord(char** values, int nColumns) {
	NodeRecord newRecord = (NodeRecord)allocateBytes(sizeof(struct Record));
	int i;
	if (!(newRecord)) { return NULL; }
	newRecord->next = NULL;
	if (!(newRecord->values = (char**)allocateBytes(nColumns * sizeof(char*)))) { return NULL; }
	for (i = 0; i<nColumns; i++) {
		if (!(newRecord->values[i] = (char*)allocateBytes(strlen(values[i]) * sizeof(char)))) { return NULL; }
		strcpy(newRecord->values[i], values[i]);
	}
	return newRecord;
}

bool insertRecordDb(Table t, NodeRecord r) {
	// table or record are not initialized, impossible to insert the record
	if (!t || !r) { return false; }
	int i;

	// insert the record into the head of the list of record
	r->next = t->recordList;
	t->recordList = r;

	// insert the element in each tree
	for (i = 0; i < t->nColumns; i++) {
		if (!(insertNodeTree(&(t->treeList[i]), createNodeRBT(r)))) { return false; }
	}
	return true;
}

QueryResultList querySelect(Table t, ParseResult res) {

	QueryResultList* queryToGet = (QueryResultList*)malloc(sizeof(QueryResultList));
	*queryToGet = NULL;
	int keyIndex;
	if (res->queryType != SELECT_WITHOUT_FILTERS) {
		keyIndex = searchColumnIndex(t, res->keyName);
		if (keyIndex == -1) { return NULL; }
	}

	switch (res->queryType) {
	case (ORDER_BY):
		selectOrderBy(t->treeList[keyIndex].root, queryToGet, res->order);
		break;
	case (GROUP_BY):
		selectOrderBy(t->treeList[keyIndex].root, queryToGet, res->order);
		countForGroupBy(keyIndex, (*queryToGet));
		break;
	case(WHERE):
		selectWhere(t->recordList, queryToGet, keyIndex, res->querySelector, res->key);
		break;
	case(SELECT_WITHOUT_FILTERS):
		selectNoFilter(t->recordList, queryToGet);
	default:
		break;
	}

	//Deallocating the pointer to the query result list 
	QueryResultList ret = *queryToGet;
	free(queryToGet);
	queryToGet = NULL;
	return ret;
}

void unsuccessfulParse(ParseResult result, int errorCode) {
	
	result->success = false;
	result->parseErrorCode = errorCode;
}

bool charIsAllowed(char c, const char * forbiddenCharSet) {
	int i;
	for (i = 0; forbiddenCharSet[i]; i++) {
		if (c == forbiddenCharSet[i]) {
			return false;
		}
	}
	return true;
}

int parseQueryParameter(char * query, char ** parameter, const char * forbiddenCharSet) {
	const int paramSize = 1024;
	int i;

	// parameter = where to save the parsed parameter
	*parameter = (char *)malloc(sizeof(char) * paramSize);

	if (!*parameter) return -1;
	
	for (i = 0; query[i] && i<paramSize - 1; i++) {
		if (charIsAllowed(query[i], forbiddenCharSet)) {
			// add it to the parsed parameter
			(*parameter)[i] = query[i];
		}
		else {
			(*parameter)[i] = '\0';
			// found not-allowed char
			return i;
		}
	}

	(*parameter)[i] = '\0';
	return i;
}

int parseQueryType(char * query) {
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

	for (i=0; i<3; i++) { 
		for (j = 0; query[j] && queryType[i][j]; j++) {
			if (query[j] != queryType[i][j]) {
				break;
			}
		}
		if (queryType[i][j] == 0) {
			return queryDefinedValues[i];
		}
	}
	return NO_QUERY;
}

// error code class 100
ParseResult parseQueryCreateTable(char * query, ParseResult result) {
	const char * paramForbiddenChars = " ,.;*%$#@&^~\"'=+/\n\r!?()[]{}<>";
	const char space = ' ';
	const char comma = ',';

	result->queryType = CREATE_TABLE;

	// CREATE TABLE name
	//             ^ position 12
	query += 12;

	// checking the space
	if (*query != ' ') {
		unsuccessfulParse(result, 101);
		return result;
	}

	query++; // first char of tableName

			 // parsing table name and shifting forward the pointer
	query += parseQueryParameter(query, &(result->tableName), paramForbiddenChars);

	if (!result->tableName) {
		unsuccessfulParse(result, 102);
		return result;
	}

	if (*query != ' ') {
		unsuccessfulParse(result, 103);
		return result;
	}

	query++;
	if (*query != '(') {
		unsuccessfulParse(result, 104);
		return result;
	}
	query++;

	result->nColumns = 128;

	result->columns = (char **)malloc(result->nColumns * sizeof(char *));

	if (!result->columns) {
		unsuccessfulParse(result, 105);
		return result;
	}

	int i = 0;
	for (i = 0; true; i++) {
		if (i >= result->nColumns) {
			char ** newColumns = (char **)realloc(result->columns, 2 * result->nColumns * sizeof(char *));

			if (!newColumns) {
				unsuccessfulParse(result, 106);
				return result;
			}

			result->nColumns *= 2;
			result->columns = newColumns;
		}

		int offset = parseQueryParameter(query, &(result->columns[i]), paramForbiddenChars);

		if (offset == -1) {
			unsuccessfulParse(result, 111);
			return result;
		}
		else {
			query += offset;
		}

		if (*query == ',') {
			query++;
			continue;
		}

		if (*query == ')' && *(query + 1) == ';') {
			i++;

			// shrink columns name array to save space
			char ** reallocation = (char **)realloc(result->columns, i * sizeof(char*));

			if (!reallocation) {
				unsuccessfulParse(result, 107);
				return result;
			}

			result->columns = reallocation;
			result->nColumns = i;
			result->success = true;

		}
		else {
			unsuccessfulParse(result, 108);
		}

		return result;
	}

	unsuccessfulParse(result, 112);
	return result;
}

// error code class 200
ParseResult parseQueryInsertInto(char * query, ParseResult result) {
	const char * paramForbiddenChars = " ,.;*%$#@&^~\"'=+/\n\r!?()[]{}<>";
	const char space = ' ';
	const char comma = ',';

	result->queryType = INSERT_INTO;

	// INSERT INTO name
	//            ^ position 11
	query += 11;

	if (*query != ' ') {
		unsuccessfulParse(result, 201);
		return result;
	}
	query++;

	// parsing table name and shifting forward the pointer
	query += parseQueryParameter(query, &(result->tableName), paramForbiddenChars);

	if (!result->tableName) {
		unsuccessfulParse(result, 202);
		return result;
	}

	if (*query != ' ') {
		unsuccessfulParse(result, 203);
		return result;
	}
	query++;

	if (*query != '(') {
		unsuccessfulParse(result, 204);
		return result;
	}
	query++;

	// arbitrary number of columns
	result->nColumns = 128;

	result->columns = (char **)malloc(result->nColumns * sizeof(char *));

	if (!result->columns) {
		unsuccessfulParse(result, 205);
		return result;
	}

	int i;

	// gettin' those column names
	for (i = 0; true; i++) {

		// if there isn't enough space
		if (i >= result->nColumns) {
			// double up!
			char ** newColumns = (char **)realloc(result->columns, 2 * result->nColumns * sizeof(char*));

			if (!newColumns) {
				unsuccessfulParse(result, 206);
				return result;
			}

			result->nColumns *= 2;
			result->columns = newColumns;
		}

		// now there is space for sure
		// and we parse the next parameter
		query += parseQueryParameter(query, &(result->columns[i]), paramForbiddenChars);
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
			char ** reallocation = (char **)realloc(result->columns, i * sizeof(char*));

			if (!reallocation) {
				unsuccessfulParse(result, 207);
				return result;
			}

			result->columns = reallocation;
			result->nColumns = i;
		}
		else {
			unsuccessfulParse(result, 208);
		}

		break;
	}
	query++;

	// check for " VALUES ("
	const char values[] = " VALUES (";

	for (i = 0; i<9; i++) {
		if (query[i] != values[i]) {
			unsuccessfulParse(result, 209);
			return result;
		}
	}

	query += 9; // shift that pointer!

	// we expect N columns and N values
	// we already have nColumns so let's use it to init the array
	result->fieldValues = (char **)malloc(sizeof(char *) * result->nColumns);

	if (!result->fieldValues) {
		unsuccessfulParse(result, 210);
		return result;
	}

	int contFieldsValues = 0;

	for (i = 0; i<result->nColumns; i++) {
		contFieldsValues++;
		query += parseQueryParameter(query, &(result->fieldValues[i]), paramForbiddenChars);

		// comma = gotta read another value
		if (*query == ',') {
			query++;
			continue;
		}

		// closed bracket, no more values
		if (*query == ')' && *(query + 1) == ';') {
			if (result->nColumns == contFieldsValues)
				result->success = true;
			else
				result->success = false;
			return result;
		}
	}
	unsuccessfulParse(result, 211);
	return result;
}

// error code class 300
ParseResult parseQuerySelect(char * query, ParseResult result) {
	const char * paramForbiddenChars = " ,.;%$#@&^~\"'=+/\n\r!?()[]{}<>";
	const char space = ' ';
	const char comma = ',';

	result->queryType = SELECT_WITHOUT_FILTERS;

	// SELECT name
	//       ^ position 6
	query += 6;

	// checking the space
	if (*query != ' ') {
		unsuccessfulParse(result, 301);
		return result;
	}

	query++;

	// SELECT * FROM ...
	//        ^
	if (*query == '*') {
		result->nColumns = 1;

		result->columns = (char **)malloc(sizeof(char *));

		if (result->columns) {
			result->columns[0] = (char *)malloc(sizeof(char) * 2);

			if (!result->columns[0]) {
				unsuccessfulParse(result, 302);
				return result;
			}

			result->columns[0][0] = '*';
			result->columns[0][1] = '\0';

			query++;
		}
		else {
			unsuccessfulParse(result, 303);
			return result;
		}

	}
	else {
		// arbitrary number of columns
		result->nColumns = 128;

		result->columns = (char **)malloc(result->nColumns * sizeof(char *));

		if (!result->columns) {
			unsuccessfulParse(result, 304);
			return result;
		}

		int i = 0;

		// gettin' those column names
		for (i = 0; true; i++) {

			// if there isn't enough space in result->columns
			if (i >= result->nColumns) {
				// double up!
				char ** newColumns = (char **)realloc(result->columns, 2 * result->nColumns * sizeof(char*));

				if (!newColumns) {
					unsuccessfulParse(result, 305);
					return result;
				}

				result->nColumns *= 2;
				result->columns = newColumns;
			}

			// now there is space for sure
			// and we parse the next parameter
			query += parseQueryParameter(query, &(result->columns[i]), paramForbiddenChars);
			// shifting the pointer @ the same time

			// comma = gotta read another column name
			if (*query == ',') {
				query++;
				continue;
			}

			// closed bracket, no more column names
			if (*query == ' ') {
				i++;

				// now we can shrink the column list to fit, and save memory
				char ** reallocation = (char **)realloc(result->columns, i * sizeof(char*));

				if (!reallocation) {
					unsuccessfulParse(result, 306);
					return result;
				}

				result->columns = reallocation;
				result->nColumns = i;
			}
			else {
				unsuccessfulParse(result, 307);
				return result;
			}
			break;
		}
	}

	// check for " FROM "
	// SELECT sborn FROM banana;
	//             ^^^^^^
	const char values[] = " FROM ";

	int i = 0;

	for (i = 0; i<6; i++) { // 6 is the string length
		if (query[i] != values[i]) {
			unsuccessfulParse(result, 308);
			return result;
		}
	}

	query += 6; // shift that pointer!

	// parse table name
	query += parseQueryParameter(query, &(result->tableName), paramForbiddenChars);

	if (!result->tableName) {
		unsuccessfulParse(result, 309);
		return result;
	}

	// SELECT * FROM banana;
	//                     ^
	if (*query == ';') {
		result->queryType = SELECT_WITHOUT_FILTERS;
		result->success = true;
		return result;
	}

	if (*query != ' ') {
		unsuccessfulParse(result, 310);
		return result;
	}

	query++;

	// SELECT * FROM banana WHERE
	//                      ^

	switch (*query) {
	case 'W': // WHERE
		return parseQuerySelectWHERE(query, result);

	case 'O': // ORDER BY
		return parseQuerySelectORDERBY(query, result);

	case 'G': // GROUP BY
		return parseQuerySelectGROUPBY(query, result);

	default:
		unsuccessfulParse(result, 311);
		return result;
	}
}

// error code class 400
ParseResult parseQuerySelectWHERE(char * query, ParseResult result) {
	const char * paramForbiddenChars = " ,.;*%$#@&^~\"'=+/\n\r!?()[]{}<>";
	const char * whereString = "WHERE ";
	const int whereStringLength = 6;
	int i = 0, j = 0;

	result->queryType = WHERE;

	for (i = 0; i<whereStringLength; i++, query++) {
		if (*query != whereString[i]) {
			unsuccessfulParse(result, 401);
			return result;
		}
	}

	query += parseQueryParameter(query, &(result->keyName), paramForbiddenChars);

	const char operators[][5] = {
		" < ",
		" <= ",
		" == ",
		" >= ",
		" > "
	};

	const int operatorValue[] = {
		LESSER,
		LESSER_EQUAL,
		EQUAL,
		GREATER_EQUAL,
		GREATER
	};

	const int operatorLength[] = { 3, 4, 4, 4, 3 };
	const int operatorNumber = 5;

	for (i = 0; i<operatorNumber; i++) {
		for (j = 0; operators[i][j]; j++) {
			if (query[j] != operators[i][j]) {
				break;
			}
		}

		if (operators[i][j] == 0) { // meaning it finished, didn't break out
			result->querySelector = operatorValue[i];
			query += operatorLength[i];
			break;
		}
	}

	if (result->querySelector == NO_OPERATOR) {
		unsuccessfulParse(result, 402);
		return result;
	}

	query += parseQueryParameter(query, &(result->key), paramForbiddenChars);

	if (query[0] == ';') {
		result->success = true;
		return result;
	}

	unsuccessfulParse(result, 403);
	return result;
}

// error code class 500
ParseResult parseQuerySelectGROUPBY(char * query, ParseResult result) {
	const char * paramForbiddenChars = " ,.;*%$#@&^~\"'=+/\n\r!?()[]{}<>";
	const char * groupByString = "GROUP BY ";
	const int groupByStringLength = 9;
	int i = 0;

	result->queryType = GROUP_BY;

	for (i = 0; i<groupByStringLength; i++, query++) {
		if (*query != groupByString[i]) {
			unsuccessfulParse(result, 501);
			return result;
		}
	}

	query += parseQueryParameter(query, &(result->keyName), paramForbiddenChars);

	if (*query == ';') {
		result->success = true;
		return result;
	}

	unsuccessfulParse(result, 502);
	return result;
}

// error code class 600
ParseResult parseQuerySelectORDERBY(char * query, ParseResult result) {
	const char * paramForbiddenChars = " ,.;*%$#@&^~\"'=+/\n\r!?()[]{}<>";
	const char * orderByString = "ORDER BY ";
	const int orderByStringLength = 9;
	int i = 0, j = 0;

	result->queryType = ORDER_BY;

	for (i = 0; i<orderByStringLength; i++, query++) {
		if (*query != orderByString[i]) {
			unsuccessfulParse(result, 601);
			return result;
		}
	}

	query += parseQueryParameter(query, &(result->keyName), paramForbiddenChars);

	// SELECT * FROM banana ORDER BY giovanni ASC
	//                                       ^

	const char * orderASC = " ASC;";
	const char * orderDESC = " DESC;";

	if (strcmp(query, orderASC) == 0) {
		result->success = true;
		result->order = ASC;
		return result;
	}

	if (strcmp(query, orderDESC) == 0) {
		result->success = true;
		result->order = DESC;
		return result;
	}

	unsuccessfulParse(result, 602);
	return result;
}

// error code class 0
ParseResult parseQuery(char* queryString) {
	ParseResult result = (ParseResult)malloc(sizeof(struct ParseResult));

	if (!result) {
		return NULL;
	}

	// init result
	result->success = false;
	result->tableName = NULL;
	result->queryType = NO_QUERY;
	result->querySelector = NO_OPERATOR;
	result->keyName = NULL;
	result->key = NULL;
	result->columns = NULL;
	result->nColumns = 0;
	result->fieldValues = NULL;
	result->order = 0;

	result->parseErrorCode = 0;

	char * paramForbiddenChars = " ,.;*%$#@&^~\"'=+/\n\r!?()[]{}<>";

	int queryType = parseQueryType(queryString);

	switch (queryType) {
	case CREATE_TABLE:
		parseQueryCreateTable(queryString, result);
		break;
	case INSERT_INTO:
		parseQueryInsertInto(queryString, result);
		break;
	case SELECT:
		parseQuerySelect(queryString, result);
		break;
	default:
		unsuccessfulParse(result, 1);
	}

	return result;
}

void freeParseResult(ParseResult res) {
	if (res->tableName != NULL) {
		free(res->tableName);
		res->tableName = NULL;
	}
	if (res->keyName != NULL) {
		free(res->keyName);
		res->keyName = NULL;
	}
	if (res->key != NULL) {
		free(res->key);
		res->key = NULL;
	}
	if (res->columns != NULL) {
		int i;
		for (i = 0; i < res->nColumns; i++) {
			free(res->columns[i]);
			res->columns[i] = NULL;
		}
		free(res->columns);
		res->columns = NULL;
	}
	if (res->fieldValues != NULL) {
		int i;
		for (i = 0; i < res->nColumns; i++) {
			free(res->fieldValues[i]);
			res->fieldValues[i] = NULL;
		}
		free(res->fieldValues);
		res->fieldValues = NULL;
	}
	free(res);
	res = NULL;
}

void freeQueryResultList(QueryResultList res) {
	if (res != NULL) {
		freeQueryResultList(res->next);
		free(res);
		res = NULL;
	}
}

void generateLog(ParseResult pRes, char* query, QueryResultList records, Database db) {
	char* buffer = (char*)malloc(sizeof(char) * (strlen(LOG_FILE_NAME) + 1));
	strcpy(buffer, LOG_FILE_NAME);

	Table t = searchTableDb(db, pRes->tableName);
	if (t == NULL) {
		free(buffer);
		return;
	}

	FILE* f = fopen(buffer, "a+");

	if (f == NULL) {
		free(buffer);
		return;
	}

	fprintf(f, "%s\n", query);

	//Inserting header
	fprintf(f, "TABLE %s COLUMNS ", pRes->tableName);
	int i;
	if (pRes->queryType == GROUP_BY) {
		fprintf(f, "%s,COUNT;\n", pRes->keyName);
	}
	else {
		if (pRes->nColumns == 1 && pRes->columns[0][0] == '*') {
			//Print all column identificators
			for (i = 0; i < t->nColumns; i++) {
				fprintf(f, "%s", t->columns[i]);
				if (i != t->nColumns - 1)
					fprintf(f, ",");
				else
					fprintf(f, ";\n");
			}
		}
		else {
			//Print only selected column identificators
			for (i = 0; i < pRes->nColumns; i++) {
				fprintf(f, "%s", pRes->columns[i]);
				if (i != pRes->nColumns - 1)
					fprintf(f, ",");
				else
					fprintf(f, ";\n");
			}
		}
	}

	//Inserting records
	while (records != NULL) {
		fprintf(f, "ROW ");
		if (pRes->queryType == GROUP_BY) {
			int colIndex = 0;
			colIndex = searchColumnIndex(t, pRes->keyName);
			if (colIndex == -1) {
				fclose(f);
				free(buffer);
				return;
			}
			fprintf(f, "%s,%d;\n", records->nodeValue->values[colIndex], records->occurrence);
		}
		else {
			if (pRes->nColumns == 1 && pRes->columns[0][0] == '*') {
				//Print all columns
				for (i = 0; i < t->nColumns; i++) {
					fprintf(f, "%s", records->nodeValue->values[i]);
					if (i != t->nColumns - 1)
						fprintf(f, ",");
					else
						fprintf(f, ";\n");
				}
			}
			else {
				for (i = 0; i < pRes->nColumns; i++) {
					int colIndex = i;
					colIndex = searchColumnIndex(t, pRes->columns[i]);
					if (colIndex == -1) {
						free(buffer);
						fclose(f);
						return;
					}
					fprintf(f, "%s", records->nodeValue->values[colIndex]);
					if (i != pRes->nColumns - 1)
						fprintf(f, ",");
					else
						fprintf(f, ";\n");
				}
			}
		}
		records = records->next;
	}

	fprintf(f, "\n");
	fclose(f);
	free(buffer);
}

bool createTableFile(char* name, char** columns, int nColumns) {
	char* buffer = (char*)malloc(strlen(name) + 5);
	strcpy(buffer, name);
	strcat(buffer, ".txt");

	FILE* f = fopen(buffer, "w");
	if (f == NULL) {
		free(buffer);
		return false;
	}

	//Printing header
	fprintf(f, "TABLE %s COLUMNS ", name);
	//Printing columns
	int i;
	for (i = 0; i < nColumns; i++) {
		fprintf(f, "%s", columns[i]);
		if (i != nColumns - 1)
			fprintf(f, ",");
		else
			fprintf(f, ";");
	}

	fclose(f);
	free(buffer);
	return true;
}

Table loadTableFromFile(Database db, char* name) {
	char* buffer = (char*)malloc(sizeof(char) * (strlen(name) + 5));
	strcpy(buffer, name);
	strcat(buffer, ".txt");

	FILE* f = fopen(buffer, "r");
	if (f == NULL) {
		free(buffer);
		return NULL;
	}

	Table t = NULL;

	//Read table name and columns
	char header[] = { "TABLE " };
	char headerSeparator[] = { " COLUMNS " };
	char inlineSeparator = ',';
	char endlineSeparator = ';';

	//Check first header
	buffer = (char*)realloc(buffer, sizeof(char) * (strlen(header) + 1));
	buffer = fgets(buffer, strlen(header) + 1, f);
	buffer[strlen(header)] = '\0';

	if (strcmp(buffer, header) != 0) {
		free(buffer);
		fclose(f);
		return NULL;
	}

	//Check table name
	buffer = (char*)realloc(buffer, sizeof(char) * (strlen(name) + 1));
	buffer = fgets(buffer, strlen(name) + 1, f);
	buffer[strlen(name)] = '\0';

	if (strcmp(buffer, name) != 0) {
		free(buffer);
		fclose(f);
		return NULL;
	}

	//Check header separator
	buffer = (char*)realloc(buffer, sizeof(char) * (strlen(headerSeparator) + 1));
	buffer = fgets(buffer, strlen(headerSeparator) + 1, f);
	buffer[strlen(headerSeparator)] = '\0';

	if (strcmp(buffer, headerSeparator) != 0) {
		free(buffer);
		fclose(f);
		return NULL;
	}

	//Reading fields
	char** columns = NULL;
	int nColumns = 0;

	char c = 0;
	do {
		c = fgetc(f);
		if (c == EOF) {
			free(buffer);
			fclose(f);
			return NULL;
		}
		nColumns++;
		columns = (char**)realloc(columns, sizeof(char*) * nColumns);
		char* column = NULL;
		int colLen = 0;
		while (c != inlineSeparator  && c != endlineSeparator) {
			if (c == EOF) {
				free(buffer);
				fclose(f);
				return NULL;
			}
			colLen++;
			column = (char*)realloc(column, sizeof(char) * colLen);
			column[colLen - 1] = c;
			c = fgetc(f);
		}
		if (colLen == 0) {
			free(buffer);
			fclose(f);
			return NULL;
		}
		//Insert terminal char
		column = (char*)realloc(column, sizeof(char) * colLen + 1);
		column[colLen] = '\0';
		columns[nColumns - 1] = column;
	} while (c != endlineSeparator);

	c = fgetc(f);

	t = createTableDb(db, name, columns, nColumns);
	if (t == NULL) {
		free(buffer);
		fclose(f);
		return NULL;
	}

	//Reading rows
	char rowHeader[] = { "ROW " };
	char** row = NULL;

	if (c == EOF) {
		fclose(f);
		free(buffer);
		return t;
	}

	int i;
	do {
		//Check row header
		buffer = (char*)realloc(buffer, sizeof(char) * (strlen(rowHeader) + 1));
		fgets(buffer, strlen(rowHeader) + 1, f);
		buffer[strlen(rowHeader)] = '\0';
		if (strcmp(buffer, rowHeader) != 0) {
			free(buffer);
			fclose(f);
			return NULL;
		}

		row = (char**)allocateBytes(sizeof(char*) * nColumns);

		//Reding row
		for (i = 0; i < nColumns; i++) {
			char* value = NULL;
			int size = 0;
			c = fgetc(f);
			if (c == EOF) {
				free(buffer);
				fclose(f);
				return NULL;
			}
			while (c != inlineSeparator && c != endlineSeparator) {
				if (c == EOF) {
					free(buffer);
					fclose(f);
					return NULL;
				}
				size++;
				value = (char*)realloc(value, sizeof(char) * size);
				value[size - 1] = c;
				c = fgetc(f);
			}
			value = (char*)realloc(value, sizeof(char) * (size + 1));
			value[size] = '\0';
			if (c == endlineSeparator) {
				if (nColumns - 1 != i) {
					free(buffer);
					fclose(f);
					return NULL;
				}
			}
			row[i] = value;
		}

		if (c != endlineSeparator) {
			free(buffer);
			fclose(f);
			return NULL;
		}

		if (!insertRecordDb(t, createRecord(row, nColumns))) {
			free(buffer);
			fclose(f);
			return NULL;
		}
		c = fgetc(f);
		if (c != '\n' && c == EOF)
			break;
		if (c != '\n') {
			free(buffer);
			fclose(f);
			return NULL;
		}
	} while (fpeek(f) != EOF);

	free(buffer);
	buffer = NULL;
	fclose(f);
	return t;
}

bool insertIntoTableFile(char* name, char** columns, char** values, int nColumns) {
	char* buffer = (char*)malloc(sizeof(char) * (strlen(name) + 5));
	strcpy(buffer, name);
	strcat(buffer, ".txt");

	FILE* f = fopen(buffer, "r");
	if (f == NULL) {
		free(buffer);
		fclose(f);
		return false;
	}

	//Read table name and columns
	char header[] = { "TABLE " };
	char headerSeparator[] = { " COLUMNS " };
	char inlineSeparator = ',';
	char endlineSeparator = ';';

	//Check first header
	buffer = (char*)realloc(buffer, sizeof(char) * (strlen(header) + 1));
	buffer = fgets(buffer, strlen(header) + 1, f);
	buffer[strlen(header)] = '\0';

	if (strcmp(buffer, header) != 0) {
		free(buffer);
		fclose(f);
		return false;
	}

	//Check table name
	buffer = (char*)realloc(buffer, sizeof(char) * (strlen(name) + 1));
	buffer = fgets(buffer, strlen(name) + 1, f);
	buffer[strlen(name)] = '\0';

	if (strcmp(buffer, name) != 0) {
		free(buffer);
		fclose(f);
		return false;
	}

	//Check header separator
	buffer = (char*)realloc(buffer, sizeof(char) * (strlen(headerSeparator) + 1));
	buffer = fgets(buffer, strlen(headerSeparator) + 1, f);
	buffer[strlen(headerSeparator)] = '\0';

	if (strcmp(buffer, headerSeparator) != 0) {
		free(buffer);
		fclose(f);
		return false;
	}

	//Check column names and positions
	int i;
	for (i = 0; i < nColumns; i++) {
		char c = fgetc(f);
		unsigned int cont = 0;
		while (c != ',' && c != ';') {
			if (c == EOF) {
				free(buffer);
				fclose(f);
				return false;
			}
			cont++;
			buffer = (char*)realloc(buffer, (cont + 1) * sizeof(char));
			buffer[cont - 1] = c;
			c = fgetc(f);
		}
		if (c == ',' && i == nColumns - 1) {
			free(buffer);
			fclose(f);
			return false;
		}
		if (c == ';' && i != nColumns - 1) {
			free(buffer);
			fclose(f);
			return false;
		}
		buffer[cont] = '\0';

		char* first;
		char* second;
		if (cont > strlen(columns[i])) {
			first = buffer;
			second = columns[i];
		}
		else {
			first = columns[i];
			second = buffer;
		}

		if (strcmp(first, second) != 0) {
			free(buffer);
			fclose(f);
			return false;
		}
	}
	fclose(f);

	//Append into file
	buffer = (char*)realloc(buffer, sizeof(char) * (strlen(name) + 5));
	strcpy(buffer, name);
	strcat(buffer, ".txt");

	f = fopen(buffer, "a");
	if (f == NULL) {
		free(buffer);
		fclose(f);
		return false;
	}

	//Writing header
	fprintf(f, "\nROW ");

	//Writing values
	for (i = 0; i < nColumns; i++) {
		fprintf(f, "%s", values[i]);
		if (i == nColumns - 1)
			fputc(';', f);
		else
			fputc(',', f);
	}

	free(buffer);
	fclose(f);
	return true;
}

void initMemoryHeap() {
	memoryHeap = (TableHeap)malloc(sizeof(struct TableHeap));
	memoryHeap->array = (TableHeapElement*)malloc(2 * sizeof(TableHeapElement));
	memoryHeap->size = 1;
	memoryHeap->last = 0;
}

TableHeapElement insertMemoryHeap(Table t) {
	if (memoryHeap->last == memoryHeap->size) {
		memoryHeap->size *= 2;
		memoryHeap->array = (TableHeapElement*)realloc(memoryHeap->array, memoryHeap->size * sizeof(TableHeapElement) + 1);
		if (memoryHeap->array == NULL)
			return NULL;
	}

	TableHeapElement newElement = (TableHeapElement)malloc(sizeof(struct TableHeapElement));
	newElement->tableReference = t;
	newElement->priority = priorityCounter++;
	newElement->memorySize = 0;
	newElement->position = memoryHeap->last + 1;

	memoryHeap->array[memoryHeap->last + 1] = newElement;
	memoryHeap->last = memoryHeap->last + 1;
	if (memoryHeap->last != 1)
		bubbleUpHeapElement(newElement);
	return newElement;
}

TableHeapElement extractMemoryHeap() {
	TableHeapElement res = memoryHeap->array[1];
	int last = memoryHeap->last;
	if (last == 0)
		return NULL;
	res->position = -1;
	if (last == 1) {
		memoryHeap->array[1] = NULL;
	}
	else {
		swapHeapElement(1, last);
		memoryHeap->array[last] = NULL;
		memoryHeap->last = last - 1;
		if (last > 2)
			bubbleDownHeapElement(memoryHeap->array[1]);
	}
	return res;
}

void updatePriorityMemoryHeap(TableHeapElement element, unsigned long long int priority) {
	if (priority < element->priority) {
		element->priority = priority;
		bubbleUpHeapElement(element);
	}
	else if (priority > element->priority) {
		element->priority = priority;
		bubbleDownHeapElement(element);
	}
}

// Secondary functions implementation
int strCompare(char * a, char * b) {
	int res = strcmp(a, b);

	if (res < 0) {
		return LESSER;
	}
	if (res > 0) {
		return GREATER;
	}

	return EQUAL;
}

int strIsNumber(char * s) {
	int size = 0;
	int i = 0;
	int isNumber = true;

	for (i = 0; s[i]; i++, size++) {
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

int strAreBothNumbers(char * a, char * b) {
	// useless comment
	return strIsNumber(a) && strIsNumber(b);
}

double parseDouble(char * s) {
	int i = 0;
	int separatorFound = false;
	int exponent = -1;
	int iStart = 0;

	double signMultiplier = 1.0;
	double result = 0;

	if (s[0] == '-') {  // if negative
		signMultiplier = -1.0;  // at the end, multiply by -1
		iStart = 1; // start the conversion from 1 (0 is '-')
	}

	// increment by one the exponent 'till the separator or terminator is found
	for (i = iStart; s[i] != '.' && s[i]; i++, exponent++) {}

	// convert the number
	for (i = iStart; s[i]; i++, exponent--) {
		if (s[i] == '.') { // you want to skip the '.'
			exponent++; // and compensate the exponent decrementation
			continue;
		}

		// char to int times the correct ten power, casted as double
		result += (double)((s[i] - '0') * pow(10, exponent));
	}

	return result * signMultiplier;
}

int compare(char * a, char * b) {  // compares two strings
	double numA = 0, numB = 0;

	if (strAreBothNumbers(a, b)) {
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

	return strCompare(a, b);
}

Node createNodeRBT(void* r) {
	Node x;
	if (!(x = (Node)allocateBytes(sizeof(struct RBTNode)))) { return NULL; }
	x->nodeValue = r;
	return x;
}


bool nodeCompare(int columnIndex, void * nodeA, void * nodeB) {
	bool isTable = (columnIndex == TABLE);

	if (isTable) {
		Table tableA = (Table)nodeA;
		Table tableB = (Table)nodeB;
		return compare(tableA->name, tableB->name);
	}
	NodeRecord recordA = (NodeRecord)nodeA;
	NodeRecord recordB = (NodeRecord)nodeB;

	return compare(recordA->values[columnIndex], recordB->values[columnIndex]);
}

bool insertNodeTree(Tree T, Node z) {
	if (!z || !T) return false;
	z->head = T;
	Node y = NULL;
	Node x = T->root;

	while (x) {
		y = x;
		if (nodeCompare(T->key, z->nodeValue, y->nodeValue) == LESSER) {
			x = x->left;
		}
		else {
			x = x->right;
		}
	}

	z->p = y;

	if (!y) {
		T->root = z;
	}
	else {
		if (nodeCompare(T->key, z->nodeValue, y->nodeValue) == LESSER) {
			y->left = z;
		}
		else {
			y->right = z;
		}
	}

	z->left = NULL;
	z->right = NULL;
	z->color = RED;

	return rbtInsertFixup(T, z);
}

bool rbtInsertFixup(Tree T, Node z) {
	if (!T || !z) { return false; }

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
					if (!(leftRotate(T, z))) { return false; }
				}
				z->p->color = BLACK;
				z->p->p->color = RED;
				if (!(rightRotate(T, z->p->p))) { return false; }
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
					if (!(rightRotate(T, z))) { return false; }
				}
				z->p->color = BLACK;
				z->p->p->color = RED;
				if (!(leftRotate(T, z->p->p))) { return false; }
			}
		}
	}
	T->root->color = BLACK;
	return true;
}

bool leftRotate(Tree T, Node x) {
	if (!T || !x) { return false; }
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
	if (!T || !x) { return false; }

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

void selectOrderBy(Node x, QueryResultList* queryToGet, int order) {
	if (!x) { return; }
	if (order != ASC && order != DESC) { return; }

	// I have to walk into the tree in the opposite way because i push elements to the head of the list
	if (order == ASC) {
		selectOrderBy(x->right, queryToGet, order);
	}
	else {
		selectOrderBy(x->left, queryToGet, order);
	}

	QueryResultList newElement;
	if (!(newElement = (QueryResultList)malloc(sizeof(struct QueryResultElement)))) { return; }
	newElement->next = (*queryToGet);
	newElement->nodeValue = (NodeRecord)(x->nodeValue);
	newElement->occurrence = 1;
	*queryToGet = newElement;

	if (order == ASC) {
		selectOrderBy(x->left, queryToGet, order);
	}
	else {
		selectOrderBy(x->right, queryToGet, order);
	}
}

void countForGroupBy(int key, QueryResultList queryToGet) {
	QueryResultList temp = queryToGet, temp2;
	while (temp && temp->next) {
		if (compare(temp->nodeValue->values[key], temp->next->nodeValue->values[key]) == EQUAL) {
			temp->occurrence++;
			temp2 = temp->next;
			temp->next = temp2->next;
			free(temp2);
		}
		else {
			temp = temp->next;
		}
	}
}

void selectWhere(NodeRecord r, QueryResultList* queryToGet, int keyIndex, int querySelector, char* key) {
	if (!r || keyIndex<0 || querySelector<0 || querySelector>4 || !key || !queryToGet) { return; }

	int comparison = compare(r->values[keyIndex], key);
	bool addToList = false;

	switch (querySelector) {
	case(GREATER):
		if (comparison == GREATER) { addToList = true; }
		break;

	case(GREATER_EQUAL):
		if (comparison == GREATER || comparison == EQUAL) { addToList = true; }
		break;

	case(EQUAL):
		if (comparison == EQUAL) { addToList = true; }
		break;

	case(LESSER_EQUAL):
		if (comparison == LESSER || comparison == EQUAL) { addToList = true; }
		break;

	case(LESSER):
		if (comparison == LESSER) { addToList = true; }
		break;
	default:
		break;
	}
	if (addToList) {
		QueryResultList newElement;
		if (!(newElement = (QueryResultList)malloc(sizeof(struct QueryResultElement)))) { return; }
		newElement->next = (*queryToGet);
		*queryToGet = newElement;
		newElement->occurrence = 1;
		newElement->nodeValue = r;
	}
	selectWhere(r->next, queryToGet, keyIndex, querySelector, key);
	return;
}

void selectNoFilter(NodeRecord r, QueryResultList* queryToGet) {
	if (!r || !queryToGet) { return; }

	QueryResultList newElement;
	if (!(newElement = (QueryResultList)malloc(sizeof(struct QueryResultElement)))) { return; }
	newElement->next = (*queryToGet);
	*queryToGet = newElement;
	newElement->occurrence = 1;
	newElement->nodeValue = r;
	selectNoFilter(r->next, queryToGet);
	return;
}


int searchColumnIndex(Table T, char* key) {
	int i = 0;
	while (i < T->nColumns) {
		if (compare(key, T->columns[i]) == EQUAL) { return i; }
		i++;
	}
	return -1;
}

int fpeek(FILE * const fp) {
	const int c = getc(fp);
	return c == EOF ? EOF : ungetc(c, fp);
}

void* allocateBytes(int bytes) {
	//static variable that keeps track of heap size
	static int memoryUsage = 0;

	memoryUsage += bytes;

	//if memory gets over the threshold and we have more than one table allocated, dellocate
	while (memoryUsage > MEMORY_THRESHOLD && moreThanOneTableAllocated()) {
		int tableSize = deallocateFurthestTable();
		memoryUsage -= tableSize;
	}

	void* res = NULL;
	res = malloc(bytes);

	//same thing if OS returns NULL
	while (res == NULL && moreThanOneTableAllocated()) {
		int tableSize = deallocateFurthestTable();
		memoryUsage -= tableSize;
		res = malloc(bytes);
	}

	if (currentTableUsed != NULL)
		currentTableUsed->heapReference->memorySize += bytes;

	return res;
}

bool moreThanOneTableAllocated() {
	return memoryHeap->last >= 2;
}

int deallocateFurthestTable() {
	TableHeapElement furthest = extractMemoryHeap();
	if (furthest == NULL)
		return 0;
	int res = furthest->memorySize;
	deallocateTable(database, furthest->tableReference); // The pointer to db is needed!
	free(furthest);
	return res;
}

void swapHeapElement(int a, int b) {
	TableHeapElement tmp = memoryHeap->array[a];
	memoryHeap->array[a] = memoryHeap->array[b];
	memoryHeap->array[b] = tmp;
	memoryHeap->array[a]->position = b;
	memoryHeap->array[b]->position = a;
}

void bubbleUpHeapElement(TableHeapElement el) {
	int i = el->position;
	int parent;
	do {
		parent = i / 2;
		if (memoryHeap->array[i]->priority < memoryHeap->array[parent]->priority) {
			swapHeapElement(i, parent);
			i = parent;
		}
	} while (i == parent && i != 1);
}

void bubbleDownHeapElement(TableHeapElement el) {
	int min = el->position;
	TableHeapElement* a = memoryHeap->array;
	int i;
	do {
		i = min;
		int l = i * 2;
		int r = l + 1;
		if (l <= memoryHeap->last) {
			if (a[l]->priority < a[i]->priority)
				min = l;
			if (r <= memoryHeap->last && a[r] != NULL && a[r]->priority < a[min]->priority)
				min = r;
			if (i != min)
				swapHeapElement(i, min);
		}
	} while (i != min);
}

bool checkQueryIntegrity(Table t, ParseResult res) {
	if (!t || !res) { return false; }
	int qt = res->queryType, i, j;
	bool isIntact = true, tempFound;

	if (compare(t->name, res->tableName) != EQUAL) { isIntact = false; }

	switch (res->queryType) {
	case(INSERT_INTO):
		if (t->nColumns != res->nColumns) { isIntact = false; break; }
		for (i = 0; i < t->nColumns; i++) {
			if (compare(t->columns[i], res->columns[i]) != EQUAL) { isIntact = false; break; }
		}
		break;
		//  in the following cases i have to check that columns that are requested to be printed really are in the table
	case(WHERE):
	case(ORDER_BY):
		if (res->queryType == WHERE || res->queryType == ORDER_BY) {
			isIntact = false;
			for (i = 0; i < t->nColumns; i++) {
				if (compare(t->columns[i], res->keyName) == EQUAL) {
					isIntact = true;
					break;
				}
			}
		}
	case(GROUP_BY):
		if (res->queryType == GROUP_BY) {
			if (res->nColumns != 1) { isIntact = false; break; }
			if (compare(res->keyName, res->columns[0]) != EQUAL) { isIntact = false; break; }
		}
	case(SELECT_WITHOUT_FILTERS):
		if (res->nColumns > t->nColumns) { isIntact = false; break; }
		if (res->nColumns == 1 && compare(res->columns[0], "*") == EQUAL) { isIntact = true; break; }
		for (i = 0; i < res->nColumns; i++) {
			tempFound = false;
			for (j = 0; j < t->nColumns; j++) {
				if (compare(t->columns[j], res->columns[i]) == EQUAL) { tempFound = true; break; }
			}
			if (!tempFound) { isIntact = false; break; }
		}
	default: break;
	}

	return isIntact;
}
