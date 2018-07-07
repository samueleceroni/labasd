#define bool char
#define true 1
#define false 0

/*
Input: 
	-La stringa contenente la query da eseguire, ad esempio "CREATE TABLE studenti (matricola, nome, cognome)"
Output:
	-bool: true/false, se l'esecuzione della query e' andata a buon fine o meno (presenza di eventuali errori) 
*/

// General Part

bool executeQuery(char*);

// End of General Part


// DataBase Part

struct DataBaseHead{
	struct TableDB* root;
};

typedef struct DataBaseHead* DBHead;



// A Table of the Database
struct TableDB {
	char* name;
	char** columns;
	struct Record* recordList;
	struct RBTree* treeList;
	struct TableDB* next;
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
	struct rbTree* next;
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

typedef struct RBTNode Node;

struct QueryResultElement{
	int occurence;
	struct Record* nodeValue;
	struct QueryResultElement* next;
};

typedef struct QueryResultElement* QueryResultList;

// End of Database Part



//Parser
struct ParseResult {
	bool success;
	char* tableName;
	int queryType;
	int querySelector;
	char** columns;
	char** fieldValues;
};

typedef struct ParseResult* ParseResult;



///////////////
/* Functions */
///////////////


// DataBase Part

// General
void freeDB(DBHead h);



// Table
Table createTable(DBHead h, char* tableName, char** columns);
Table getTableP(DBHead h, char* tableName);



// Record, or Row of the Table
NodeRecord createRecord(char** values);
void insertIntoTable(Table t, NodeRecord r);


QueryResultList querySelect(Table t, ParseResult res);

// DataBase Part



// 
ParseResult* parseQuery(char* queryString);
void freeParseResult(ParseResult res);



//Logger
void generateLog(ParseResult res);
void generateLogSelect(ParseResult res, QueryResultList records);




// 
//ListOfRecord* bstQuery(Bst* bst, char* key, int selector);
//void bstInsert(Bst* bst, char* key, char** values);
