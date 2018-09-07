#include "lib1718.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#define ANSI_COLOR_RED     "\x1b[31m"
#define ANSI_COLOR_GREEN   "\x1b[32m"
#define ANSI_COLOR_YELLOW  "\x1b[33m"
#define ANSI_COLOR_BLUE    "\x1b[34m"
#define ANSI_COLOR_MAGENTA "\x1b[35m"
#define ANSI_COLOR_CYAN    "\x1b[36m"
#define ANSI_COLOR_RESET   "\x1b[0m"

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

void flushInput(){
	while(getchar() != '\n');
}

int main(){
	int nString = 73;
	int executeFrom = 0;
	int executeTo = 73;
	int nChar = 3000;

	char ** stringsToParse = (char **) malloc (nString * sizeof(char*));

	for (int i=0; i<nString; i++){
		stringsToParse[i] = (char *) malloc (nChar*sizeof(char));
	}

	// stringsToParse[0] = "CREATE TABLE tabella1 (t1colonna1,t1colonna2,t1colonna3);";
	// stringsToParse[1] = "INSERT INTO tabella1 (t1colonna1,t1colonna2,t1colonna3) VALUES (value1,value2,value3);";
	// stringsToParse[2] = "SELECT * FROM tabella1;";
	// stringsToParse[3] = "SELECT t1colonna1 FROM tabella1;";
	// stringsToParse[4] = "SELECT t1colonna1,t1colonna2 FROM tabella1;";
	// stringsToParse[5] = "SELECT * FROM tabella1 WHERE t1colonna1 > value1;";
	// stringsToParse[6] = "SELECT * FROM tabella1 WHERE t1colonna1 >= value1;";
	// stringsToParse[7] = "SELECT * FROM tabella1 ORDER BY t1colonna1 DESC;";
	// stringsToParse[8] = "SELECT * FROM tabella1 GROUP BY nome;";
	// stringsToParse[9] = "SELECT banana FROM palma GROUP BY colore;";
	// stringsToParse[10] = "SELECT mele,pere FROM cesto GROUP BY peso;";

	freopen ("test-query.txt", "r", stdin);

	for (int i=0; i<nString; i++) {
		fgets (stringsToParse[i], 3000, stdin);
	}

	for (int i=0; i<nString; i++) {
		int j=0;
		
		while (stringsToParse[i][j+1]) {
			j++;
		}

		stringsToParse[i][j] = 0;
		// printf("%s\n", stringsToParse[i]);
	}


	for (int i=executeFrom; i<executeTo; i++){
		// printf("--------------------------\n%d) QUERY: %s\n", i, stringsToParse[i]);
		printf("%i) %s", i+1 , stringsToParse[i]);

		ParseResult emuParsed;	// = (ParseResult) malloc(sizeof(struct ParseResult));
		emuParsed = parseQuery(stringsToParse[i]);

		if (emuParsed->success == 0) {
			printf(ANSI_COLOR_RED "FAILURE: %d\n" ANSI_COLOR_RESET, emuParsed->parseErrorCode);
		} else {
			printf(ANSI_COLOR_GREEN "SUCCESS\n" ANSI_COLOR_RESET);
		}
		printf("tableName: %s\n", emuParsed->tableName);
		printf("queryType: ");

		switch (emuParsed->queryType) {
			case CREATE_TABLE: printf ("CREATE TABLE\n"); break;
			case WHERE: printf ("WHERE\n"); break;
			case ORDER_BY: printf ("ORDER_BY\n"); break;
			case GROUP_BY: printf ("GROUP_BY\n"); break;
			case INSERT_INTO: printf ("INSERT_INTO\n"); break;
			case SELECT_WITHOUT_FILTERS: printf ("SELECT_WITHOUT_FILTERS\n"); break;
			case SELECT: printf ("SELECT\n"); break;
			case NO_QUERY: printf ("NO_QUERY\n"); break;
		}

		printf("querySelector: ");

		switch (emuParsed->querySelector) {
			case EQUAL: printf ("="); break;
			case GREATER: printf (">"); break;
			case LESSER: printf ("<"); break;
			case GREATER_EQUAL: printf (">="); break;
			case LESSER_EQUAL: printf ("<="); break;
			case NO_OPERATOR: printf ("none"); break;
		}
		printf("\n");

		printf("keyName: %s\n", emuParsed->keyName);
		printf("key: %s\n", emuParsed->key);
		printf("nColumns: %d\n", emuParsed->nColumns);

		if (emuParsed->columns){
			for (int j=0; j<emuParsed->nColumns; j++) {
				if (j > 16) {
					printf("...\n");
					printf("columns[%d]: %s\n", (emuParsed->nColumns)-1, emuParsed->columns[(emuParsed->nColumns)-1]);
					break;
				}
				printf("columns[%d]: %s\n", j, emuParsed->columns[j]);
			}
		}

		if (emuParsed->fieldValues){
			for (int j=0; j<emuParsed->nColumns; j++) {
				if (j > 16) {
					printf("...\n");
					printf("columns[%d]: %s\n", (emuParsed->nColumns)-1, emuParsed->fieldValues[(emuParsed->nColumns)-1]);
					break;
				}
				printf("fieldValues[%d]: %s\n", j, emuParsed->fieldValues[j]);
			}
		}
		printf("order: ");

		switch (emuParsed->order) {
			case 0: printf ("ASC\n"); break;
			case 1: printf ("DESC\n"); break;
		}
		printf("-----------------------------------------------------\n");

		free(emuParsed);
	}

	return 0;
}
