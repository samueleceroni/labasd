#define bool int
#define true 1
#define false 0

/*
Input: 
	-La stringa contenente la query da eseguire, ad esempio "CREATE TABLE studenti (matricola, nome, cognome)"
Output:
	-bool: true/false, se l'esecuzione della query e' andata a buon fine o meno (presenza di eventuali errori) 
*/
bool executeQuery(char*);

//Record
typedef struct Record {
	char** columns;
	char** values;
} Record;

Record* createRecord(char** columns, char** values);

//ListOfRecord
typedef struct ListOfRecord {
	Record record;
	struct ListOfRecord* next;
} ListOfRecord;

void freeListOfRecord(ListOfRecord* l);

//Bst
typedef struct Bst {
	char* key;
	int type;
	int count;
	ListOfRecord* records;

	//TODO
} Bst;

typedef struct ListOfBst {
	Bst bst;
	struct ListOfBst* next;
} ListOfBst;

ListOfRecord* bstQuery(Bst* bst, char* key, int selector);
void bstInsert(Bst* bst, char* key, char** values);

//Table
typedef struct Table {
	char* name;
	char* columns;
	ListOfRecord* records;
	ListOfBst* bsts[3];
	//TODO
} Table;

//List of table
typedef struct ListOfTable {
	Table table;
	struct ListOfTable* next;
} ListOfTable;

Table* searchTable(ListOfTable* l, char* tableName);
Table* loadTable(char* tableName);
void insertTable(ListOfTable* l, Table* t);

Table* createTable(char* tableName, char** columns);
void insertIntoTable(Table* t, Record* r);

//Parser
typedef struct ParseResult {
	bool success;
	char* tableName;
	int queryType;
	int querySelector;
	char** columns;
	char** fieldValues;
} ParseResult;

ParseResult* parseQuery(char* queryString);
void freeParseResult(ParseResult* res);
ListOfRecord* querySelect(Table* t, ParseResult* res);

//Logger
void generateLog(ParseResult* res);
void generateLogSelect(ParseResult* res, ListOfRecord* records);
