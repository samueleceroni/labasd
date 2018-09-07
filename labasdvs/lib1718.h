#define bool int
#define true 1
#define false 0

/*
Input:
-La stringa contenente la query da eseguire, ad esempio "CREATE TABLE studenti (matricola, nome, cognome)"
Output:
-bool: true/false, se l'esecuzione della query e' andata a buon fine o meno (presenza di eventuali errori)
*/

//============//
// STRUCTURES //
//============//

//Memory management
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


// DataBase
struct TableDB {
	char* name;
	char** columns;
	int nColumns;
	struct Record* recordList;
	struct RBTree* treeList; // Array of nColumns tree

	TableHeapElement heapReference; // Reference to the memory management system
};
typedef struct TableDB* Table;


struct Record {		// Record, or Row of the table
	char** values;
	struct Record* next;
};
typedef struct Record* NodeRecord;


struct RBTree {		// Head of a RedBlackTree
	int key;
	struct RBTNode* root;
};
typedef struct RBTree* Tree;
typedef struct RBTree* Database;


struct RBTNode {	// Node of a RedBlackTree
	bool color;
	void * nodeValue;	// can be table or record
	int occurrences;
	NodeRecord* allValues;
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
	int parseErrorCode;
};
typedef struct ParseResult* ParseResult;



//===========//
// Functions //
//===========//

// General Part
bool executeQuery(char*);

// DataBase Part
void initDatabase();
Table createTableDb(char* tableName, char** columns, int nColumns);
Table searchTableDb(char* tableName);
void deallocateTable(Table t);
NodeRecord createRecord(char** values, int nColumns);
bool insertRecordDb(Table t, NodeRecord r);
QueryResultList querySelect(Table t, ParseResult res);

// Memory Part
void initMemoryHeap();
TableHeapElement insertMemoryHeap(Table t);
void updatePriorityMemoryHeap(TableHeapElement element, unsigned long long int priority);
void freeParseResult(ParseResult res);
void freeQueryResultList(QueryResultList res);
void* allocateBytes(int bytes);

//Checker
bool checkQueryIntegrity(Table t, ParseResult res);

// Parser
ParseResult parseQuery(char* queryString);

//Logger
void generateLog(ParseResult pRes, char* query, QueryResultList records);

//File part
bool createTableFile(char* name, char** columns, int nColumns);
Table loadTableFromFile(char* name);
bool insertIntoTableFile(char* name, char** columns, char** values, int nColumns);

// Useful internal tools
int compare(char * a, char * b);
int powd(int base, int exp);// in order to avoid to use cmath.h, not explicitly allowed