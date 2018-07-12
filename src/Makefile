CC=gcc
CFLAGS=-I$(IDIR)

IDIR =../include
BDIR=bin
ODIR=obj
TDIR=test

DIRS=$(BDIR) $(ODIR)

_DEPS = lib1718.h
DEPS = $(patsubst %,$(IDIR)/%,$(_DEPS))

_OBJ = lib1718.o
OBJ = $(patsubst %,$(ODIR)/%,$(_OBJ))

_MAIN = main.o
MAIN = $(patsubst %,$(ODIR)/%,$(_MAIN))

_TEST = test.o
TEST = $(patsubst %,$(ODIR)/%,$(_TEST))

$(DIRS):
	mkdir -p $@

$(ODIR)/%.o: %.c $(DEPS) $(DIRS)/
	$(CC) -c -o $@ $< $(CFLAGS) $(TFLAGS) $(DFLAGS)
$(ODIR)/test.o: $(TDIR)/test.c $(DIRS)/
	$(CC) -c -o $@ $< $(CFLAGS)

labasd: $(OBJ) $(MAIN)
	$(CC) -o $(BDIR)/$@ $^ $(CFLAGS)

test: TFLAGS = -D TEST
test: $(OBJ) $(TEST)
	$(CC) -o $(BDIR)/$@ $^ $(CFLAGS)

testdebug: TFLAGS = -D TEST
testdebug: DFLAGS = -D DEBUG
testdebug: $(OBJ) $(TEST)
	$(CC) -o $(BDIR)/$@ $^ $(CFLAGS)

lib: lib1718.c
	$(CC) -c -o $(ODIR)/lib1718.o $^ $(CFLAGS)

.PHONY: clean

clean:
	rm -f $(BDIR)/*  $(ODIR)/*.o *~ core $(INCDIR)/*~ 
