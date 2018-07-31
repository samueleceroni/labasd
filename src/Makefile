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
MKDIR_P = mkdir -p

.PHONY: directories
directories: ${BDIR} ${ODIR}

${BDIR}:
	${MKDIR_P} ${BDIR}
${ODIR}:
	${MKDIR_P} ${ODIR}

$(ODIR)/%.o: %.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS) $(TFLAGS) $(DFLAGS)
$(ODIR)/test.o: $(TDIR)/test.c
	$(CC) -c -o $@ $< $(CFLAGS)

labasdfulldebug: DFLAGS = -D DEBUG -D MIN_DEBUG
labasdfulldebug: directories clabasd

labasdmindebug: DFLAGS = -D MIN_DEBUG
labasdmindebug: directories clabasd

labasdtestfulldebug: TFLAGS = -D TEST -D MIN_DEBUG -D DEBUG
labasdtestfulldebug: directories clabasd

labasdtestmindebug: TFLAGS = -D TEST -D MIN_DEBUG
labasdtestmindebug: directories clabasd

labasdtest: TFLAGS = -D TEST
labasdtest: directories clabasd

labasd: directories clabasd 
clabasd: $(OBJ) $(MAIN)
	$(CC) -o $(BDIR)/$@ $^ $(CFLAGS)

test: TFLAGS = -D TEST
test: directories ctest

ctest: $(OBJ) $(TEST)
	$(CC) -o $(BDIR)/$@ $^ $(CFLAGS)

testdebug: TFLAGS = -D TEST
testdebug: DFLAGS = -D DEBUG
testdebug: directories ctestdebug
ctestdebug: $(OBJ) $(TEST)
	$(CC) -o $(BDIR)/$@ $^ $(CFLAGS)

lib: directories lib1718.c
	$(CC) -c -o $(ODIR)/lib1718.o lib1718.c $(CFLAGS)

all: directories lib test labasd

.PHONY: clean

clean:
	rm -f $(BDIR)/*  $(ODIR)/*.o *~ core $(INCDIR)/*~ 
