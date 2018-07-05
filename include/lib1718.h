#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>

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

//Defines
#define EQUAL 0
#define GREATER 1
#define LESSER 2
#define GREATER_EQUAL 3
#define LESSER_EQUAL 4

//Bst struct definition
struct STRUCT_BST {
    //TODO
};
typedef struct STRUCT_BST BST;
//Bst functions definition

//PARAMS:
//BST = tree, char* = key to search, int = type of search (equal, greater, ...)
//RETURN VAL:
//Generic pointer to the list of rows found
void* bst_query(BST, char*, int);

//PARAMS:
//BST = tree, char* = key to insert, char** = pointer to list of fields
//RETURN VAL:
//true = succeded, false = failed
bool bst_insert(BST, char*, char**);

//Table struct definition
struct STRUCT_TABLE {
    //TODO
};
typedef struct STRUCT_TABLE TABLE;
//Table functions definition

//PARAMS:
//TABLE = table to do the query, char* = query string
//RETURN VAL:
//true = succeded, false = failed
bool table_query(TABLE, char*);
