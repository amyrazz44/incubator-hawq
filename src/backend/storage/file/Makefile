#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for storage/file
#
# IDENTIFICATION
#    $PostgreSQL: pgsql/src/backend/storage/file/Makefile,v 1.13 2008/02/19 10:30:08 petere Exp $
#
#-------------------------------------------------------------------------

subdir = src/backend/storage/file
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = fd.o buffile.o bfz.o pipe.o compress_nothing.o compress_zlib.o \
	   gp_compress.o filesystem.o

include $(top_srcdir)/src/backend/common.mk
