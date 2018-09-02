#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "lib1718.h"


void removeReturn(char* s) {
	if (s[strlen(s) - 1] == '\n')
		s[strlen(s) - 1] = '\0';
}

void flushInput() {
	while (getchar() != '\n');
}

int main() {
	int i=0, nFalse = 0; 
	FILE *f = fopen("queries.txt", "r");
	bool exit = false;
	//while (!exit) {
	for(i=0; i<12000; i++){
		char buffer[201];
		fgets(buffer, 200, f);
		removeReturn(buffer);
		bool res = executeQuery(buffer);
		if (i % 100 == 0) printf("%d\n", i);
		if (!res) { nFalse++; printf("%d:FALSE:%s\n", i, buffer);}
		/*
			if (res)
				printf("Query andata a buon fine!\n");
			else
				printf("Query non andata a buon fine...\n");
			printf("Vuoi eseguire un'altra query? (S/N): ");
			char resp = getchar();
			if (resp == 'N')
				exit = true;
			flushInput();
		*/
	}
	/*
	i++;
	bool res = executeQuery("INSERT INTO huge (a,b,c,d,e,f,g,h,i,j) VALUES (a,b,a,b,a,b,a,b,a,b);");
	if (i % 100 == 0) printf("%d\n", i);
	if (!res) { nFalse++; printf("%d:FALSE:%s\n", i, "INSERT INTO huge (a,b,c,d,e,f,g,h,i,j) VALUES (a,b,a,b,a,b,a,b,a,b);"); }
	i++;
	res = executeQuery("SELECT * FROM huge;");
	if (i % 100 == 0) printf("%d\n", i);
	if (!res) { nFalse++; printf("%d:FALSE:%s\n", i, "SELECT * FROM huge;"); }
	i++;
	res = executeQuery("SELECT * FROM huge ORDER BY a ASC;");
	if (i % 100 == 0) printf("%d\n", i);
	if (!res) { nFalse++; printf("%d:FALSE:%s\n", i, "SELECT * FROM huge;"); }
	*/
	printf("%d\n", nFalse);
	system("pause");
	return 0;
}
