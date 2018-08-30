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

// struct DatabaseHead{
// 	struct TableDB* table;
// 	struct DatabaseHead* next;
// };

//typedef struct DatabaseHead* Database;

//Memory management structs
struct TableHeapElement {
	struct TableDB* tableReference;
	unsigned long long int priority;
	int memorySize;
	int position;
};

typedef struct TableHeapElement* TableHeapElement;

struct TableHeap {
	TableHeapElement* array;
	int last;
	int size;
};

typedef struct TableHeap* TableHeap;
//End of memory management structs

// A Table of the Database
struct TableDB {
	char* name;
	char** columns;
	int nColumns;
	struct Record* recordList;
	struct RBTree* treeList; // Array of nColumns tree

	TableHeapElement heapReference; // Reference to the memory management system
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
typedef struct RBTree* Database;


// Node of a RedBlackTree
struct RBTNode {
	bool color;
	void * nodeValue;	// can be table or record
	struct RBTree* head;
	struct RBTNode* p;
	struct RBTNode* right;
	struct RBTNode* left;

};

typedef struct RBTNode* Node;

struct QueryResultElement {
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
	int parseErrorCode;
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

// Memory Part
void initMemoryHeap();
TableHeapElement insertMemoryHeap(Table t);
TableHeapElement extractMemoryHeap();
void updatePriorityMemoryHeap(TableHeapElement element, unsigned long long int priority);

// Table
Table createTableDb(Database db, char* tableName, char** columns, int nColumns);
Table searchTableDb(Database db, char* tableName);
void deallocateTable(Database db, Table t);

// Record, or Row of the Table
NodeRecord createRecord(char** values, int nColumns);
bool insertRecordDb(Table t, NodeRecord r);

QueryResultList querySelect(Table t, ParseResult res);
bool checkQueryIntegrity(Table t, ParseResult res);

// Parser
ParseResult parseQuery(char* queryString);
void freeParseResult(ParseResult res);
void freeQueryResultList(QueryResultList res);

//Logger
void generateLog(ParseResult pRes, char* query, QueryResultList records, Database db);

//File part
bool createTableFile(char* name, char** columns, int nColumns);
Table loadTableFromFile(Database db, char* name);
bool insertIntoTableFile(char* name, char** columns, char** values, int nColumns);