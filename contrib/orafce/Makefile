MODULE_big = orafunc
OBJS= convert.o file.o datefce.o magic.o others.o plvstr.o plvdate.o shmmc.o plvsubst.o utility.o plvlex.o alert.o pipe.o sqlparse.o putline.o assert.o plunit.o random.o aggregate.o

DATA_built = orafunc.sql
DATA = uninstall_orafunc.sql 
DOCS = README.orafunc COPYRIGHT.orafunc INSTALL.orafunc
REGRESS = orafunc dbms_output files
REGRESS_OPTS = --load-language=plpgsql

EXTRA_CLEAN = sqlparse.c sqlparse.h sqlscan.c y.tab.c y.tab.h orafunc.sql orafunc.sql.in

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/orafce
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

ifeq ($(enable_nls), yes)
ifeq ($(PORTNAME),win32)
SHLIB_LINK += -lintl
else
SHLIB_LINK += -L$(libdir)/gettextlib
endif
endif

plvlex.o: sqlparse.o

sqlparse.o: $(srcdir)/sqlscan.c                                                                                                                      

$(srcdir)/sqlparse.h: $(srcdir)/sqlparse.c ;

$(srcdir)/sqlparse.c: sqlparse.y
ifdef BISON
	$(BISON) -d $(BISONFLAGS) -o $@ $<
else
ifdef YACC
	$(YACC) -d $(YFLAGS) -p cube_yy $<
	mv -f y.tab.c sqlparse.c
	mv -f y.tab.h sqlparse.h
else
	@$(missing) bison $< $@
endif
endif

$(srcdir)/sqlscan.c: sqlscan.l
ifdef FLEX
	$(FLEX) $(FLEXFLAGS) -o'$@' $<
else
	@$(missing) flex $< $@
endif

distprep: $(srcdir)/sqlparse.c $(srcdir)/sqlscan.c

maintainer-clean:
	rm -f $(srcdir)/sqlparse.c $(srcdir)/sqlscan.c

ifndef MAJORVERSION
MAJORVERSION := $(basename $(VERSION))
endif

$(srcdir)/datefce.c: tdhfunc.c

$(srcdir)/orafunc.sql.in: orafunc-gp.sql
	cat $< > $@
