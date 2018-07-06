#define EQUAL 0
#define GREATER 1
#define LESSER 2
#define GREATER_EQUAL 3
#define LESSER_EQUAL 4

#include "compare.h"
#include <stdio.h>

void testcompare () {
	char * test[] = {
		"a", "a",
		"aa", "aa",
		"aaa", "abc",
		"123", "123",
		"111", "112",
		"100.1", "100.1",
		"100.3", "103.0",
		"0666", "666",
		"ddd", "da",
		"555", "123",
		"-1", "-1",
		"123", "123abc",
		"123", "-123"
	};

	int result[] = {
		EQUAL,
		EQUAL,
		LESSER,
		EQUAL,
		LESSER,
		EQUAL,
		LESSER,
		EQUAL,
		GREATER,
		GREATER,
		EQUAL,
		LESSER,
		GREATER
	};

	int i=0, j=0, size=13;

	for (i=0, j=0; i<size*2; i+=2, j++) {
		int out = compare (test[i], test[i+1]);

		if (out == result[j]) {
			printf("OK | %s\n", test[i]);
		} else {
			printf("%d instead of %d | %s - %s\n", out, result[j], test[i], test[i+1]);
		}
	}
}



void testparse () {
	char * test[] = {
		"1.0",
		"0.0",
		"-1.0",
		"123.123",
		"111.111",
		"-123.111",
		"01234"
	};

	int testSize = 7;

	double result[] = {1.0, 0.0, -1.0, 123.123, 111.111, -123.111, 1234};

	int i=0;

	for (i=0; i<testSize; i++) {
		double output = parseDouble (test[i]);

		if (output == result[i]) {
			printf("%f - OK\n", output);
		} else {
			printf("%.20f instead of %.20f\n", output, result[i]);
		}
	}
}

int main () {

	// testparse();
	testcompare();

}