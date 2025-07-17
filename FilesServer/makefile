CC = gcc # el compilador será gcc
CFLAGS = -Wall # opciones de compilación: -Wall para advertencias

all: p2-dataProgram

p2-dataProgram: p2-dataProgram.o
	$(CC) $(CFLAGS) p2-dataProgram.o -o p2-dataProgram

p1-dataProgram.o: p2-dataProgram.C
	$(CC) $(CFLAGS) -c p2-dataProgram.C

clean:
	rm -f *.o p2-dataProgram
