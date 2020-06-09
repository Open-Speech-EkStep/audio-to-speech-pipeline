# Made by Chanwoo Kim
# chanwook@cs.cmu.edu chanwcom@gmail.com
#
# Feb 26, 2006 
#
	

CPP 		= g++
CC              = gcc 

# SUFFIX rule



CFLAGS = 	${HEADER_PATH}\
		-O3 -m32
		

HEADER_PATH 	=-I../Header\
		-I../src/libs3decoder\
		-I../src\
		-I../src/libutil\
		-I../src/ANModel
#########################################################
#
# Suffix rule
#

.SUFFIXES : .o .c .cpp
.cpp.o:
	${CPP} ${CFLAGS} -c $<
.c.o:
	${CC}  ${CFLAGS} -c $<


CPPSRCS = 	../Source/AudioFile.cpp\
 		../Source/Config.cpp\
 		../Source/Main.cpp\
 		../Source/Table.cpp\
 		../Source/SNREst.cpp\

CSRCS = \
../src/libs3decoder/fe_sigproc.c\
../src/libutil/case.c\
../src/libutil/ckd_alloc.c\
../src/libutil/cmd_ln.c\
../src/libutil/err.c\
../src/libutil/glist.c\
../src/libutil/hash.c\
../src/libutil/info.c\

#COBJS   = ${CSRCS:.c=.o}
#CPPOBJS = ${CPPSRCS:.cpp=.o}

OBJS= \
AudioFile.o\
case.o\
ckd_alloc.o\
cmd_ln.o\
Config.o\
err.o\
fe_sigproc.o\
glist.o\
hash.o\
info.o\
Main.o\
SNREst.o\
Table.o\

TARGET = WADASNR

${TARGET}:${OBJS}
	${CPP} ${CFLAGS} -o ../Exe/${TARGET} ${OBJS}

${OBJS}:${CSRCS} ${CPPSRCS}
	${CC} ${CFLAGS} -c ${CSRCS}
	${CPP} ${CFLAGS} -c ${CPPSRCS}

clean:
	rm -fr *.o *.core ../Exe/${TARGET}
