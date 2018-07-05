IDIR =../include
CC=gcc
CFLAGS=-I$(IDIR)

BDIR=bin
ODIR=obj
TDIR=test

_DEPS = lib1718.h
DEPS = $(patsubst %,$(IDIR)/%,$(_DEPS))

_OBJ = lib1718.o
OBJ = $(patsubst %,$(ODIR)/%,$(_OBJ))

_MAIN = main.o
MAIN = $(patsubst %,$(ODIR)/%,$(_MAIN))

_TEST = test.o
TEST = $(patsubst %,$(ODIR)/%,$(_TEST))

$(ODIR)/%.o: %.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS)
$(ODIR)/test.o: $(TDIR)/test.c
	$(CC) -c -o $@ $< $(CFLAGS)

labasd: $(OBJ) $(MAIN)
	$(CC) -o $(BDIR)/$@ $^ $(CFLAGS)

test: $(OBJ) $(TEST)
	$(CC) -o $(BDIR)/$@ $^ $(CFLAGS)

.PHONY: clean

clean:
	rm -f $(BDIR)/*  $(ODIR)/*.o *~ core $(INCDIR)/*~ 
