#include <string.h>
#include <stdio.h>
#include <math.h>
#include "compare.h"

#ifndef EQUAL
#define EQUAL 0
#endif

#ifndef GREATER
#define GREATER 1
#endif

#ifndef LESSER
#define LESSER 2
#endif

#ifndef GREATER_EQUAL
#define GREATER_EQUAL 3
#endif

#ifndef LESSER_EQUAL
#define LESSER_EQUAL 4
#endif

#ifndef DECIMAL_SEPARATOR
#define DECIMAL_SEPARATOR '.'
#endif

#ifndef true
#define true 1
#endif

#ifndef false
#define false 0
#endif

int strCompare (char * a, char * b) {	
	int res = strcmp (a, b);

	if (res < 0) {
		return LESSER;
	}

	if (res > 0) {
		return GREATER;
	}

	return EQUAL;
}

int strIsNumber (char * s) {
	int size = 0;
	int i = 0;
	int isNumber = true;

	for (i=0; s[i]; i++, size++) {
		if (s[0] == '-') {
			continue;
		}

		if ((s[i] < '0' || s[i] > '9') && s[i] != DECIMAL_SEPARATOR) {
			isNumber = false;
			break;
		}
	}	

	return isNumber;
}



int strAreBothNumbers (char * a, char * b) {	
	return strIsNumber(a) && strIsNumber(b);
}



double parseDouble (char * s) {
	int i = 0;
	int separatorFound = false;
	int exponent = -1;
	int iStart = 0;

	double signMultiplier = 1.0;
	double result = 0;

	if (s[0] == '-') {	// if negative
		signMultiplier = -1.0;	// at the end, multiply by -1
		iStart = 1;	// start the conversion from 1 (0 is '-')
	}

	// increment by one the exponent 'till the separator or terminator is found
	for (i=iStart; s[i]!='.' && s[i]; i++, exponent++) {}

	// convert the number
	for (i=iStart; s[i]; i++, exponent--){
		if (s[i] == '.') { // you want to skip the '.'
			exponent++;	// and compensate the exponent decrementation
			continue;
		}

		// char to int times the correct ten power, casted as double
		result += (double) ((s[i] - '0') * pow(10, exponent));
	}

	return result * signMultiplier;
}


// compares two strings

int compare (char * a, char * b) {	
	double numA = 0, numB = 0;

	if (strAreBothNumbers (a, b)) {
		numA = parseDouble(a);
		numB = parseDouble(b);

		if (numA > numB) {
			return GREATER;
		}

		if (numA < numB) {
			return LESSER;
		}

		return EQUAL;
	}

	return strCompare (a, b);
}