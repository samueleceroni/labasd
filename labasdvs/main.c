#include "lib1718.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

void removeReturn(char* s) {
	if (s[strlen(s) - 1] == '\n')
		s[strlen(s) - 1] = '\0';
}

void flushInput() {
	while (getchar() != '\n');
}

int main() {
	bool exit = false;
	while (!exit) {
		printf("Inserire la query da lanciare:\n");
		char buffer[201];
		fgets(buffer, 200, stdin);
		removeReturn(buffer);
		bool res = executeQuery(buffer);
		if (res)
			printf("Query andata a buon fine!\n");
		else
			printf("Query non andata a buon fine...\n");
		printf("Vuoi eseguire un'altra query? (S/N): ");
		char resp = getchar();
		if (resp == 'N')
			exit = true;
		flushInput();
	}
	return 0;
}
