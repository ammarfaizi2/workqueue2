CC ?= gcc
CXX ?= g++
CFLAGS =  -Wall -Wextra -O2 -ggdb3 -std=c11 -pedantic-errors
CXXFLAGS = -Wall -Wextra -O2 -ggdb3 -std=c++11 -pedantic-errors
LDFLAGS = -lpthread

ifeq ($(SANITIZE),1)
override CFLAGS += -fsanitize=address -fsanitize=undefined
override CXXFLAGS += -fsanitize=address -fsanitize=undefined
override LDFLAGS += -fsanitize=address -fsanitize=undefined
endif

ifeq ($(LTO),1)
override CFLAGS += -flto
override CXXFLAGS += -flto
override LDFLAGS += -flto
endif

HEADER_DEPS := \
	workqueue.h \
	thread.h

OBJ-y := \
	main.o \
	thread.o \
	workqueue.o

all: main

$(OBJ-y): $(HEADER_DEPS)

%.o: %.c
	$(CC) $(CFLAGS) -c -o $@ $<

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c -o $@ $<

%.o: %.cc
	$(CXX) $(CXXFLAGS) -c -o $@ $<

main: $(OBJ-y)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

clean:
	rm -vf main *.o

.PHONY: all clean
