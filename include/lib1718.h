#define bool char
#define true 1
#define false 0

/*
Input: 
	-La stringa contenente la query da eseguire, ad esempio "CREATE TABLE studenti (matricola, nome, cognome)"
Output:
	-bool: true/false, se l'esecuzione della query e' andata a buon fine o meno (presenza di eventuali errori) 
*/

// DataBase Part

struct DatabaseHead{
	struct TableDB* table;
	struct DatabaseHead* next;
};

typedef struct DatabaseHead* Database;

// A Table of the Database
struct TableDB {
	char* name;
	char** columns;
	int nColumns;
	struct Record* recordList;
	struct RBTree* treeList; // Array of nColumns tree
};

typedef struct TableDB* Table;

//Record, or Row of the table
struct Record {
	char** values;
	struct Record* next;
};

typedef struct Record* NodeRecord;

// Head of a RedBlackTree
struct RBTree {
	int key;
	struct RBTNode* root;
};

typedef struct RBTree* Tree;

// Node of a RedBlackTree
struct RBTNode {
	bool color;
	struct Record* nodeValue;
	struct RBTree* head;
	struct RBTNode* p;
	struct RBTNode* right;
	struct RBTNode* left;

};

typedef struct RBTNode* Node;

struct QueryResultElement{
	int occurrence;
	struct Record* nodeValue;
	struct QueryResultElement* next;
};

typedef struct QueryResultElement* QueryResultList;

// End of Database Part

//Parser
struct ParseResult {
	bool success;
	char* tableName;
	int queryType; // type of select
	int querySelector;
	char* keyName;
	char* key;
	char** columns;
	int nColumns;
	char** fieldValues;
	int order; // asc or desc
	// look at queryselect function in order to understand how to parse commands for select query
};

typedef struct ParseResult* ParseResult;

///////////////
/* Functions */
///////////////

// General Part
bool executeQuery(char*);
// End of General Part


// DataBase Part
void initDatabase(Database* db);

// Table
Table createTableDb(Database db, char* tableName, char** columns, int nColumns);
Table searchTableDb(Database db, char* tableName);

// Record, or Row of the Table
NodeRecord createRecord(char** values, int nColumns);
bool insertRecordDb(Table t, NodeRecord r);

QueryResultList querySelect(Table t, ParseResult res);

ParseResult parseQuery(char* queryString);
void freeParseResult(ParseResult res);
void freeQueryResultList(QueryResultList res);

//Logger
void generateLog(ParseResult pRes, char* query, QueryResultList records, Database db);

//File part
bool checkTable(char* name);
bool createTableFile(char* name, char** columns);
Table loadTableFromFile(Database db, char* name);
