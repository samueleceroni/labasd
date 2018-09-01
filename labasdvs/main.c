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
	int i;
	FILE *f = fopen("queries.txt", "r");
	bool exit = false;
	//while (!exit) {
	for(i=0; i<12000; i++){
		if(i%10 == 0) printf("%d\n", i);
		char buffer[201];
		fgets(buffer, 200, f);
		removeReturn(buffer);
		bool res = executeQuery(buffer);
/*		if (res)
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
	return 0;
}
