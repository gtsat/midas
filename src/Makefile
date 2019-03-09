CC      =        g++

CFLAGS  =        -std=gnu11 -g3 -O0 -fPIC -pedantic -Werror-implicit-function-declaration -Wall
#CFLAGS	=        -std=gnu11 -DNDEBUG -O2 -fPIC -pedantic -Werror-implicit-function-declaration -Wall

OBJECTS =        Node.o \
                 ServerSocket.o ClientSocket.o Socket.o \
                 Pool.o Dtree.o

LIBS    =        -lpthread -lm 

all               : main 
main              : common.h $(OBJECTS) Pool.h Node.h Dtree.h $(LIBS)
Node.o            : Node.h ServerSocket.h ClientSocket.h Pool.h common.h
ServerSocket.o    : ServerSocket.h Socket.h
ClientSocket.o    : ClientSocket.h Socket.h
Socket.o          : Socket.h
Pool.o            : Pool.h Dtree.h
Dtree.o           : Dtree.h bpriority_queue.h


.PHONY  : all clean

clean   :
		-rm -f qprocessor main $(OBJECTS) 

