#CC=c99

CFLAGS=-Wall -g -fPIC

all: libutil.so demo

map.o: map.h
queue.o: queue.h
demo.o: map.o queue.o

libutil.so: map.o queue.o
	$(CC) $(CFLAGS) -shared -o $@ $^ -lpthread

demo: demo.o libutil.so
	$(CC) -o $@ $< ./libutil.so -lpthread

clean:
	rm -f *.o libutil.so demo
