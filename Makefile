##########################################################################
#
#                foreign-data wrapper for CouchDB
#
# Copyright (c) 2011, PostgreSQL Global Development Group
#
# This software is released under the PostgreSQL Licence
#
# Author: Zheng Yang <zhengyang4k@gmail.com>
#
# IDENTIFICATION
#                 couchdb_fdw/Makefile
#
##########################################################################

MODULE_big = couchdb_fdw
OBJS = couchdb_fdw.o

EXTENSION = couchdb_fdw
DATA = couchdb_fdw--1.0.sql

REGRESS = couchdb_fdw

EXTRA_CLEAN = sql/couchdb_fdw.sql expected/couchdb_fdw.out

SHLIB_LINK = -lcurl -lyajl

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/couchdb_fdw
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

