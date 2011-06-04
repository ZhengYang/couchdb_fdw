/*----------------------------------------------------------
 *
 *          foreign-data wrapper for CouchDB
 *
 * Copyright (c) 2011, PostgreSQL Global Development Group
 *
 * This software is released under the PostgreSQL Licence
 *
 * Author: Zheng Yang <zhengyang4k@gmail.com>
 *
 * IDENTIFICATION
 *        couchdb_fdw/couchdb_fdw.c
 *
 *----------------------------------------------------------
 */

#include "postgres.h"

#include <string.h>

#include "funcapi.h"
#include "access/reloptions.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_user_mapping.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "utils/memutils.h"
#include "utils/builtins.h"
#include "utils/relcache.h"
#include "storage/lock.h"
#include "miscadmin.h"

#include "curl/curl.h"
#include <yajl/yajl_parse.h>
#include <yajl/yajl_common.h>
#include <yajl/yajl_gen.h>

PG_MODULE_MAGIC;

#define PROCID_TEXTEQ 67

/*
 * A string buffer to store id's.
 */
static int BUFFER_SIZE = 5;


/*
 * information for ForeignScanState.fdw_state
 */

typedef struct CouchDBFdwExecutionState
{
	/* A index pointing to current id to retrieve */
	int cursor;
	AttInMetadata *attinmeta;
	char	*address;
	int		port;
	char	*database;
	char	*username;
	char	*password;
	int		buffer_size;
	StringInfoData *id_buffer;
	StringInfoData *columns;
	int	num_of_columns;
	long long int	total_rows;
	long long int	offset;
	List	*qual_list;
	bool	qual_scanned;
} CouchDBFdwExecutionState;

/*
 * Context used to 
 */
typedef struct context {
	int level;
	int current_index;
	char *map_key;
	/* 
	 * TODO: change it to enum. 
	 * This stores which RESTful service are we sending, 
	 * so that diff parsing rules will be applied in the call back 
	 */
	int request_type;
} context;

typedef struct dbsize_context {
	long int doc_count;
	char *map_key;
} dbsize_context;

typedef struct alldocs_context {
	char *map_key;
	long long int total_rows;
	long long int offset;
	int counter;
	StringInfoData *ids;
} alldocs_context;

typedef struct doc_context {
	int depth;
	int column_index;
	int num_of_columns;
	char *map_key;
	StringInfoData *column_list;
	StringInfoData *column_data;
	yajl_gen gen;
	yajl_gen doc_gen;
	yajl_alloc_funcs *funcs;
} doc_context;

typedef struct couch_doc {
	char *doc_id;
	char *doc_rev;
	char *doc;
} couch_doc;

typedef struct curl_data {
	char *doc;
	yajl_handle *handle;
} curl_data;





/*
 * FDW option
 */
struct CouchDBFdwOption
{
	const char  *optname;
	Oid     optcontext;
};

/*
 * Array of valid options, the rest will be assumed as column->json attribute mapping.
 */
static struct CouchDBFdwOption valid_options[] = 
{
	/* Foreign server options */
	{ "address",    ForeignServerRelationId },
	{ "port",       ForeignServerRelationId },
	
	/* Foreign table options */
	{ "database",   ForeignTableRelationId },
	
	/* User Mapping options */
	{ "username",	UserMappingRelationId},
	{ "password",	UserMappingRelationId},
	
	/* Sentinel */
	{ NULL,         InvalidOid}
};


/*
 * SQL functions
 */
extern Datum couchdb_fdw_handler(PG_FUNCTION_ARGS);
extern Datum couchdb_fdw_validator(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(couchdb_fdw_handler);
PG_FUNCTION_INFO_V1(couchdb_fdw_validator);

static void couchdbGetDatabaseSize(const char *address, const int port, const char *database, 
								   const char *username, const char *password, int *dbsize);
/*
 * FDW callback routines
 */
static FdwPlan *couchdbPlanForeignScan(Oid foreigntableid, PlannerInfo *root, RelOptInfo *baserel);
static void couchdbExplainForeignScan(ForeignScanState *node, ExplainState *es);
static void couchdbBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *couchdbIterateForeignScan(ForeignScanState *node);
static void couchdbReScanForeignScan(ForeignScanState *node);
static void couchdbEndForeignScan(ForeignScanState *node);

/*
 * helper function
 */
void* palloc_wrapper (void *ctx, size_t sz);
void* repalloc_wrapper (void *ctx, void *ptr, size_t sz);
void pfree_wrapper(void *ctx, void *ptr);
static void couchdbGetQual(Node *node, TupleDesc tupdesc, List *col_mapping_list, char **key, char **value, bool *pushdown);
static bool couchdbIsValidOption(const char *option, Oid context);

/*
 * couchdb API wrappers
 */

static int dbsize_handle_number(void * ctx, const char * s, size_t l);
static int dbsize_handle_map_key(void * ctx, const unsigned char * stringVal, size_t stringLen);
static size_t couchdbsize_writer(void *buffer, size_t size, size_t nmemb, void *userp);
static void couchdbGetDoc(const char *address, const int port, const char *database, 
						  const char *username, const char *password, const char *id, const char *rev, 
						  StringInfoData *columns, const int col_size, StringInfoData **column_data);
static size_t couchdbdoc_writer(void *buffer, size_t size, size_t nmemb, void *userp);
static int alldocs_handle_map_key(void * ctx, const unsigned char * stringVal, size_t stringLen);
static int alldocs_handle_string(void * ctx, const unsigned char * stringVal, size_t stringLen);
static int alldocs_handle_number(void * ctx, const char * s, size_t l);
static size_t alldocs_writer(void *buffer, size_t size, size_t nmemb, void *userp);
static void couchdbGetAllDocs(const char *address, const int port, const char *database, const char *username, const char *password,
				  const int limit, const char *startkey, const bool descending, StringInfoData *ids[], 
							  long long int *total_rows, long long int *offset);
static int doc_handle_null(void * ctx);
static int doc_handle_boolean(void * ctx, int boolean);
static int doc_handle_integer(void *ctx, long long integerVal);
static int doc_handle_double(void *ctx, double doubleVal);
static int doc_handle_number(void * ctx, const char * s, size_t l);
static int doc_handle_string(void * ctx, const unsigned char * stringVal,
							 size_t stringLen);
static int doc_handle_start_map (void *ctx);
static int doc_handle_map_key(void *ctx, const unsigned char * stringVal,
							  size_t stringLen);
static int doc_handle_end_map(void * ctx);
static int doc_handle_start_array(void * ctx);
static int doc_handle_end_array(void * ctx);

void* palloc_wrapper (void *ctx, size_t sz) {
	//MemoryContext * mctx = (MemoryContext *) ctx;
	//return MemoryContextAlloc( (MemoryContext) ctx, sz);
	return malloc(sz);
}

void* repalloc_wrapper (void *ctx, void *ptr, size_t sz) {
	return realloc(ptr, sz);
}

void pfree_wrapper(void *ctx, void *ptr) {
	free(ptr);
}

Datum
couchdb_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *fdwroutine = makeNode(FdwRoutine);

	fdwroutine->PlanForeignScan = couchdbPlanForeignScan;
	fdwroutine->ExplainForeignScan = couchdbExplainForeignScan;
	fdwroutine->BeginForeignScan = couchdbBeginForeignScan;
	fdwroutine->IterateForeignScan = couchdbIterateForeignScan;
	fdwroutine->ReScanForeignScan = couchdbReScanForeignScan;
	fdwroutine->EndForeignScan = couchdbEndForeignScan;

	PG_RETURN_POINTER(fdwroutine);
}


/*START*******************************************************************************************************/

/*
 * Validate the generic options given to a FOREIGN DATA WRAPPER, SERVER,
 * USER MAPPING or FOREIGN TABLE that uses couchdb_fdw.
 * 
 * TODO: Should there be a validator function because the options can be any 
 * string unless we have a way to retrieve the column names of the newly created
 * table. Also we need to know which statement (CREATE SERVER? CREATE FDW? or CREATE TBALE?)
 * is calling the validator because each will have its own valid options.
 */
Datum
couchdb_fdw_validator(PG_FUNCTION_ARGS)
{
	List	*options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid		catalog = PG_GETARG_OID(1);
	char	*svr_address = NULL;
	int		svr_port = 0;
	char	*svr_database = NULL;
	char	*username = NULL;
	char	*password = NULL;
	ListCell	*cell;
	
#ifdef DEBUG
	elog(NOTICE, "couchdb_fdw_validator");
#endif
	
	/*
	 * Check that the necessary options: address, port, database
	 */
	foreach(cell, options_list)
	{
		
		DefElem	   *def = (DefElem *) lfirst(cell);
		
		/* Compain invalid options */
		if (!couchdbIsValidOption(def->defname, catalog))
		{
			struct CouchDBFdwOption *opt;
			StringInfoData buf;
			
			/*
			 * Unknown option specified, complain about it. Provide a hint
			 * with list of valid options for the object.
			 */
			initStringInfo(&buf);
			for (opt = valid_options; opt->optname; opt++)
			{
				if (catalog == opt->optcontext)
					appendStringInfo(&buf, "%s%s", (buf.len > 0) ? ", " : "",
									 opt->optname);
			}
			
			ereport(ERROR, 
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME), 
					 errmsg("invalid option \"%s\"", def->defname), 
					 errhint("Valid options in this context are: %s", buf.len ? buf.data : "<none>")
					 ));
		}
		
		/* Complain about redundent options */
		if (strcmp(def->defname, "address") == 0)
		{
			if (svr_address)
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), 
								errmsg("conflicting or redundant options: address (%s)", defGetString(def))
								));
			
			svr_address = defGetString(def);
		}
		else if (strcmp(def->defname, "port") == 0)
		{
			if (svr_port)
				ereport(ERROR, 
						(errcode(ERRCODE_SYNTAX_ERROR), 
						 errmsg("conflicting or redundant options: port (%s)", defGetString(def))
						 ));
			
			svr_port = atoi(defGetString(def));
		}
		else if (strcmp(def->defname, "database") == 0)
		{
			if (svr_database)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options: database (%s)", defGetString(def))
						 ));
			
			svr_database = defGetString(def);
		}
		else if (strcmp(def->defname, "username") == 0)
		{
			if (username)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options: username (%s)", defGetString(def))
						 ));
			
			username = defGetString(def);
		}
		else if (strcmp(def->defname, "password") == 0)
		{
			if (password)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options: password (%s)", defGetString(def))
						 ));
			
			password = defGetString(def);
		}
	}
	
	PG_RETURN_VOID();
}


/*
 * Check if the provided option is one of the valid options.
 * context is the Oid of the catalog holding the object the option is for.
 */
static bool
couchdbIsValidOption(const char *option, Oid context)
{
	struct CouchDBFdwOption *opt;

#ifdef DEBUG
	elog(NOTICE, "couchdbIsValidOption");
#endif
	
	/* Check if the options presents in the valid option list */
	for (opt = valid_options; opt->optname; opt++)
	{
		if (context == opt->optcontext && strcmp(opt->optname, option) == 0)
			return true;
	}
	
	/* Foreign table may have anything as a mapping option */
	if (context == ForeignTableRelationId) 
		return true;
	else
		return false;
}



/*
 * Fetch the options for a couchdb_fdw foreign table.
 */
static void
couchdbGetOptions(Oid foreigntableid, char **address, int *port, char **database, 
				  char **username, char **password, List **mapping_list)
{
	ForeignTable	*table;
	ForeignServer	*server;
	UserMapping	*mapping;
	List		*options;
	ListCell	*lc;

#ifdef DEBUG
	elog(NOTICE, "couchdbGetOptions");
#endif
	
	/*
	 * Extract options from FDW objects.
	 */
	table = GetForeignTable(foreigntableid);
	server = GetForeignServer(table->serverid);
	mapping = GetUserMapping(GetUserId(), table->serverid);
	
	options = NIL;
	options = list_concat(options, table->options);
	options = list_concat(options, server->options);
	options = list_concat(options, mapping->options);
	
	*mapping_list = NIL;
	/* Loop through the options, and get the foreign table options */
	foreach(lc, options)
	{
		DefElem *def = (DefElem *) lfirst(lc);
		
		if (strcmp(def->defname, "address") == 0) {
			*address = defGetString(def);
			continue;
		}
		
		if (strcmp(def->defname, "port") == 0) {
			*port = atoi(defGetString(def));
			continue;
		}
		
		if (strcmp(def->defname, "database") == 0) {
			*database = defGetString(def);
			continue;
		}
		if (strcmp(def->defname, "username") == 0) {
			*username = defGetString(def);
			continue;
		}
		
		if (strcmp(def->defname, "password") == 0) {
			*password = defGetString(def);
			continue;
		}
		
		/* Column mapping goes here */
		*mapping_list = lappend(*mapping_list, def);
	}
	
#ifdef DEBUG
	elog(NOTICE, "list length: %i", (*mapping_list)->length);
#endif
	
	/* Default values, if required */
	if (!*address)
		*address = "127.0.0.1";
	
	if (!*port)
		*port = 5984;
	
	if (!*database)
		*database = "test";
	
	if (!*username)
		*username = NULL;
	
	if (!*password)
		*password = NULL;
}


/*
 * couchdbPlanForeignScan
 *		Create a FdwPlan for a scan on the foreign table
 */
static FdwPlan *
couchdbPlanForeignScan(Oid foreigntableid, PlannerInfo *root, RelOptInfo *baserel)
{
	FdwPlan	*fdwplan;
	char	*svr_address = NULL;
	int		svr_port = 0;
	char 	*svr_database = NULL;
	char	*username = NULL;
	char	*password = NULL;
	int		dbsize = 0;
	List	*col_mapping_list;

#ifdef DEBUG
	elog(NOTICE, "couchdbPlanForeignScan");
#endif
	
	/* Fetch options  */
	couchdbGetOptions(foreigntableid, &svr_address, &svr_port, &svr_database, &username, &password, &col_mapping_list);
	//elog(NOTICE, "list length: %i", col_mapping_list->length);
	
	/*
	 * Construct FdwPlan with cost estimates.
	 */
	fdwplan = makeNode(FdwPlan);
	
	/* Local databases are probably faster */
	if (strcmp(svr_address, "127.0.0.1") == 0 || strcmp(svr_address, "localhost") == 0) {
		fdwplan->startup_cost = 10;
	}
	else
		fdwplan->startup_cost = 25;
	
	fdwplan->total_cost = 100 + fdwplan->startup_cost;
	fdwplan->fdw_private = NIL;	/* not used */
	
	couchdbGetDatabaseSize(svr_address, svr_port, svr_database, username, password, &dbsize);
	
	fdwplan->total_cost = fdwplan->total_cost + dbsize;
	
#ifdef DEBUG
	elog(NOTICE, "new total cost: %f", fdwplan->total_cost);
#endif
	
	return fdwplan;
}

/*
 * couchdbBeginForeignScan
 *
 */
static void
couchdbBeginForeignScan(ForeignScanState *node, int eflags)
{
	CouchDBFdwExecutionState  *festate;
	char	*svr_address;
	int		svr_port;
	char	*svr_database;
	char	*username;
	char	*password;
	StringInfoData	*columns;
	int		num_of_columns;
	StringInfoData *id_buffer;
	Relation rel;
	long long int total_rows;
	long long int offset;
	List	*col_mapping_list;
	ListCell	*col_mapping;
	List	*qual_list;
	
	char	*qual_key = NULL;
	char	*qual_value = NULL;
	bool	equal = FALSE;
	bool	pushdown = FALSE;
	int i;
	
#ifdef DEBUG
	elog(NOTICE, "couchdbBeginForeignScan");
#endif
	
	/* fetch options */
	couchdbGetOptions(RelationGetRelid(node->ss.ss_currentRelation), &svr_address, &svr_port, 
					  &svr_database, &username, &password, &col_mapping_list);
	
	
	/* fill in id buffer */
	couchdbGetAllDocs(svr_address, svr_port, svr_database, username, password, BUFFER_SIZE, NULL, false, &id_buffer, &total_rows, &offset);
	
	
	/* fetch the table column info */
	rel = heap_open(RelationGetRelid(node->ss.ss_currentRelation), AccessShareLock);
	num_of_columns = rel->rd_att->natts;
	columns = (StringInfoData *) palloc(sizeof(StringInfoData) * num_of_columns);
	for (i = 0; i < num_of_columns; i++)
	{
		StringInfoData col;
		StringInfoData mapping;
		bool	mapped;
		
		/* retrieve the column name */
		initStringInfo(&col);
		appendStringInfo(&col, "%s", NameStr(rel->rd_att->attrs[i]->attname));
		mapped = FALSE;
		
		/* check if the column name is mapping to a different name in couchdb */
		foreach(col_mapping, col_mapping_list)
		{
			DefElem *def = (DefElem *) lfirst(col_mapping);
			if (strcmp(def->defname, col.data) == 0) {
				initStringInfo(&mapping);
				appendStringInfo(&mapping, "%s", defGetString(def));
				mapped = TRUE;
				break;
			}
		}
		
		/* decide which name is going to be used */
		if (mapped) 
			columns[i] = mapping;
		else 
			columns[i] = col;
	}
	heap_close(rel, NoLock);
	
	
	/* init fdw state and save it to the fdw state node */
	festate = (CouchDBFdwExecutionState *) palloc(sizeof(CouchDBFdwExecutionState));
	festate->cursor = 0;
	festate->address = svr_address;
	festate->port = svr_port;
	festate->database = svr_database;
	festate->username = username;
	festate->password = password;
	festate->buffer_size = BUFFER_SIZE;
	festate->id_buffer = id_buffer;
	festate->num_of_columns = num_of_columns;
	festate->columns = columns;
	festate->total_rows = total_rows;
	festate->offset = offset;
	festate->qual_list = NIL;
	festate->qual_scanned = FALSE;
	/* Store the additional state info */
	festate->attinmeta = TupleDescGetAttInMetadata(node->ss.ss_currentRelation->rd_att);
	node->fdw_state = (void *) festate;
	
#ifdef DEBUG
	for (i = 0; i<BUFFER_SIZE; i++) {
		elog(NOTICE, "%i: %s", i, id_buffer[i].data);
	}
#endif
	
	/* See if we've got a qual we can push down */
	qual_list = NIL;
	if (node->ss.ps.plan->qual)
	{
		StringInfoData	rev;
		StringInfoData	id;
		ListCell		*lc;
		
		foreach (lc, node->ss.ps.qual)
		{
			/* Only _id can be pushed down */
			ExprState  *state = lfirst(lc);
			
			couchdbGetQual((Node *) state->expr, node->ss.ss_currentRelation->rd_att, col_mapping_list, 
						   &qual_key, &qual_value, &equal);
			
			
			if (equal) {
				if (strcmp(qual_key, "_id") == 0) {
					initStringInfo(&id);
					appendStringInfo(&id, "%s", qual_value);
					pushdown = TRUE;
				}
				
				/* always push in rev first */
				if (strcmp(qual_key, "_rev") == 0) {
					initStringInfo(&rev);
					appendStringInfo(&rev, "%s", qual_value);
					qual_list = lappend(qual_list, rev.data);
				}
			}
		}
		if (pushdown)
			qual_list = lappend(qual_list, id.data);
		festate->qual_list = list_copy(qual_list);
	}
}



/*
 * couchdbIterateForeignScan
 *
 */
static TupleTableSlot *
couchdbIterateForeignScan(ForeignScanState *node)
{
	TupleTableSlot *slot;
	int cursor;
	CouchDBFdwExecutionState* fstate;
	StringInfoData	*id_buffer;
	StringInfoData	*column_data;
	StringInfoData	*columns;
	int		num_of_columns;
	char	*svr_address;
	int		svr_port;
	char	*svr_database;
	char	*username;
	char	*password;
	long long int	total_rows;
	long long int	offset;
	List	*qual_list;
	bool	qual_scanned;
	bool	endOfScan;

#ifdef DEBUG
	elog(NOTICE, "couchdbIterateForeignScan");
#endif
	
	endOfScan = false;
	
	/* obtaining the state of current scan */
	fstate = (CouchDBFdwExecutionState *) node->fdw_state;
	
	cursor = fstate->cursor;
	svr_address = fstate->address ;
	svr_port = fstate->port;
	svr_database = fstate->database;
	username = fstate->username;
	password = fstate->password;
	id_buffer = fstate->id_buffer;
	num_of_columns = fstate->num_of_columns;
	columns = fstate->columns;
	total_rows = fstate->total_rows;
	offset = fstate->offset;
	qual_list = fstate->qual_list;
	qual_scanned = fstate->qual_scanned;
	
	/* got quals to pushdown */
	if (list_length(qual_list) != 0) {
		if (qual_scanned) {
			endOfScan = TRUE;
		}
		else {
			char *_id;
			char *_rev;
			
			_id = (char *) ((list_length(qual_list) == 1) ? list_nth(qual_list, 0) : list_nth(qual_list, 1));
			_rev = (char *) ((list_length(qual_list) == 1) ? NULL : list_nth(qual_list, 1));
			
			couchdbGetDoc(svr_address, svr_port, svr_database, username, password, 
						  _id, _rev, columns, num_of_columns, &column_data);
			fstate->qual_scanned = TRUE;
		}
	}
	else {
		/* if not the last page */
		if (offset + BUFFER_SIZE < total_rows) {
			/* if the cursor is at the end of the BUFFER */
			if (cursor == BUFFER_SIZE) {
				/* retrieve new set of ids */
				char *start_key;
				start_key = id_buffer[BUFFER_SIZE - 1].data;
				pfree(id_buffer);
				id_buffer = (StringInfoData *) palloc(sizeof(StringInfoData) * BUFFER_SIZE);
				couchdbGetAllDocs(svr_address, svr_port, svr_database, username, password, BUFFER_SIZE, 
								  start_key, false, &id_buffer, &total_rows, &offset);
				fstate->id_buffer = id_buffer;
				/* reset cursor (skip the first id of next batch)*/
				cursor = 1;
				
				/* retrieve doc */
				column_data = (StringInfoData *) palloc(sizeof(StringInfoData) * num_of_columns);
				couchdbGetDoc(svr_address, svr_port, svr_database, username, password, id_buffer[cursor].data, NULL, 
							  columns, num_of_columns, &column_data);
				cursor ++;
			}
			/* else increment the cursor by 1 */
			else {
				column_data = (StringInfoData *) palloc(sizeof(StringInfoData) * num_of_columns);
				couchdbGetDoc(svr_address, svr_port, svr_database, username, password, id_buffer[cursor].data, NULL, 
							  columns, num_of_columns, &column_data);
				cursor ++;
			}
		}
		/* if on the last page */
		else {
			int remainder;
			remainder = total_rows - offset;
			/* if there are still ids to loop */
			if (cursor <= remainder - 1) {
				column_data = (StringInfoData *) palloc(sizeof(StringInfoData) * num_of_columns);
				couchdbGetDoc(svr_address, svr_port, svr_database, username, password, id_buffer[cursor].data, NULL, 
							  columns, num_of_columns, &column_data);
				cursor ++;
			}
			/* else implies no more records */
			else {
				endOfScan = true;
			}
			
		}
		/* save the cursor into the state */
		fstate->cursor = cursor;
		/* save the total_rows and offset into the state */
		fstate->total_rows = total_rows;
		fstate->offset = offset;
	}
	/* prepare for returning the tuple */
	slot = node->ss.ss_ScanTupleSlot;
	/* Cleanup */
	ExecClearTuple(slot);
	
	/* if not the end of the scan, construct the tuple */
	if (!endOfScan) {
		/*
		 * put doc into a row
		 */
		char **values;
		HeapTuple tuple;
		int i;
		
		values = (char **) palloc(sizeof(char *) * num_of_columns);
		for (i = 0; i < num_of_columns; i++) {
			values[i] = column_data[i].data;
		}
		
		tuple = BuildTupleFromCStrings(fstate->attinmeta, values);
		ExecStoreTuple(tuple, slot, InvalidBuffer, FALSE);
	}
	return slot;
}



/*
 * couchdbExplainForeignScan
 *
 */
static void
couchdbExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
	
	CouchDBFdwExecutionState *festate;
	int	dbsize;

#ifdef DEBUG
	elog(NOTICE, "couchdbExplainForeignScan");
#endif
	
	festate = (CouchDBFdwExecutionState *) node->fdw_state;
	
	/* Execute a query to get the database size */
	couchdbGetDatabaseSize(festate->address, festate->port, festate->database, festate->username, festate->password, &dbsize);
	
	/* Suppress file size if we're not showing cost details */
	if (es->costs)
	{
		ExplainPropertyLong("Foreign CouchDB Database Size", dbsize, es);
	}
}

/*
 * couchdbEndForeignScan
 *		Finish scanning foreign table and dispose objects used for this scan
 */
static void
couchdbEndForeignScan(ForeignScanState *node)
{
	CouchDBFdwExecutionState *festate;

#ifdef DEBUG
	elog(NOTICE, "couchdbEndForeignScan");
#endif
	
	festate = (CouchDBFdwExecutionState *) node->fdw_state;
	
	/* if festate is NULL, we are in EXPLAIN; nothing to do */
	if (festate)
	{
		if (festate->id_buffer)
			pfree(festate->id_buffer);
		
		if (festate->columns)
			pfree(festate->columns);
	}
}

/*
 * couchdbReScanForeignScan
 *		Rescan table, possibly with new parameters
 */
static void
couchdbReScanForeignScan(ForeignScanState *node)
{
	CouchDBFdwExecutionState *festate;

#ifdef DEBUG
	elog(NOTICE, "couchdbReScanForeignScan");
#endif
	
	festate = (CouchDBFdwExecutionState *) node->fdw_state;
	
	festate->cursor = 0;
}



static void
couchdbGetDatabaseSize(const char *address, const int port, const char *database, 
					   const char *username, const char *password, int *dbsize) 
{
	CURL	*curl;
	StringInfoData	dbsize_url;
	StringInfoData	auth_str;
	yajl_handle	handle;
	bool	hasAuth;
	dbsize_context		*ctx;
	yajl_alloc_funcs	*funcs;
	yajl_callbacks		*callbacks;
	
#ifdef DEBUG
	elog(NOTICE, "couchdbGetDatabaseInfo");
#endif
	
	if (username != NULL && password != NULL) {
		initStringInfo(&auth_str);
		appendStringInfo(&auth_str, "%s:%s@", username, password);
		hasAuth = true;
	}
	else {
		hasAuth = false;
	}
	
	initStringInfo(&dbsize_url);
	appendStringInfo(&dbsize_url, "http://%s%s:%i/%s", hasAuth ? auth_str.data : "", address, port,database );
	
#ifdef DEBUG
	elog(NOTICE, "%s", dbsize_url.data);
#endif
	
	
	callbacks = (yajl_callbacks *) palloc(sizeof(yajl_callbacks));
	callbacks->yajl_null = NULL;
	callbacks->yajl_boolean = NULL;
	callbacks->yajl_integer = NULL;
	callbacks->yajl_double = NULL;
	callbacks->yajl_number = dbsize_handle_number;
	callbacks->yajl_string = NULL;
	callbacks->yajl_start_map = NULL;
	callbacks->yajl_map_key = dbsize_handle_map_key;
	callbacks->yajl_end_map = NULL;
	callbacks->yajl_start_array = NULL;
	callbacks->yajl_end_array = NULL;
	
	
	/*
	 * Initialize YAJL Parser
	 */
	funcs = (yajl_alloc_funcs *) palloc(sizeof(yajl_alloc_funcs));
	funcs->malloc = palloc_wrapper;
	funcs->realloc = repalloc_wrapper;
	funcs->free = pfree_wrapper;
	funcs->ctx = (void *) &TopMemoryContext;
	
	ctx = (dbsize_context *) palloc(sizeof(dbsize_context));
	ctx->doc_count = 0;
	
	handle = yajl_alloc(callbacks, funcs, (void *) ctx);
	
	
	/*
	 * CURL the database url to get the database meta information
	 */
	curl = curl_easy_init();
	curl_easy_setopt(curl, CURLOPT_URL, dbsize_url.data);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, couchdbsize_writer);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, &handle);
	curl_easy_perform(curl);
	curl_easy_cleanup(curl);
	
#ifdef DEBUG
	elog(NOTICE, "num of docs: %ld", ctx->doc_count);
#endif
	
	*dbsize = ctx->doc_count;
}

static size_t
couchdbsize_writer(void *buffer, size_t size, size_t nmemb, void *userp)
{
	yajl_handle *handle = (yajl_handle *) userp;
	yajl_status stat = yajl_parse(*handle, (unsigned char *) buffer, size * nmemb);
	if (stat != yajl_status_ok) elog(NOTICE, "cannot parse");
	return size * nmemb;
}

static int dbsize_handle_number(void * ctx, const char * s, size_t l)
{	
	/* If the value's key is doc_count, store it into the id_buffer array  */
	dbsize_context *context = (dbsize_context *) ctx;
	if ( strcmp("doc_count", context->map_key) == 0 ) {
		context->doc_count = atoi(s);
	}
    return 1;
}

static int dbsize_handle_map_key(void * ctx, const unsigned char * stringVal,
								 size_t stringLen)
{
	dbsize_context *context;
	StringInfoData map_key;
	
	initStringInfo(&map_key);
	appendBinaryStringInfo(&map_key, (char *) stringVal, (int) stringLen);
	
	/* Save the map key to context, the following value parsed will belong to this key */
	context = (dbsize_context *) ctx;
	context->map_key = map_key.data;
    return 1;
}



/*
 * function to retrieve a single doc from couchdb 
 */
static void
couchdbGetDoc(const char *address, const int port, const char *database, 
			  const char *username, const char *password, const char *id, const char *rev, 
			  StringInfoData *columns, const int col_size, StringInfoData **column_data)
{
	CURL	*curl;
	StringInfoData	doc_url;
	StringInfoData	auth_str;
	StringInfoData	rev_str;
	yajl_alloc_funcs* funcs;
	yajl_callbacks* callbacks;
	yajl_handle	handle;
	doc_context *ctx;
	yajl_gen	gen;
	yajl_gen	doc_gen;
	bool	hasAuth;
	bool	hasRev;
	
#ifdef DEBUG
	elog(NOTICE, "couchdbGetDoc");
#endif
	
	/* Check if there is authen info specified */
	if (username != NULL && password != NULL) {
		initStringInfo(&auth_str);
		appendStringInfo(&auth_str, "%s:%s@", username, password);
		hasAuth = true;
	}
	else {
		hasAuth = false;
	}
	
	/* Check if there is revision number specified */
	if (rev != NULL) {
		initStringInfo(&rev_str);
		appendStringInfo(&rev_str, "?rev=%s", rev);
		hasRev = true;
	}
	else {
		hasRev = false;
	}
	
	
	/* Construct the URL we are going to curl */
	initStringInfo(&doc_url);
	appendStringInfo(&doc_url, "http://%s%s:%i/%s/%s%s", 
					 hasAuth ? auth_str.data : "", address, port, database, id, 
					 hasRev ? rev_str.data : "");
#ifdef DEBUG
	elog(NOTICE, "%s", doc_url.data);
#endif
	
	/* example: http://127.0.0.1:5984/cooldb/123456?rev=1-abc */
	
	
	callbacks = (yajl_callbacks *) palloc(sizeof(yajl_callbacks));
	callbacks->yajl_null = doc_handle_null;
	callbacks->yajl_boolean = doc_handle_boolean;
	callbacks->yajl_integer = doc_handle_integer;
	callbacks->yajl_double = doc_handle_double;
	callbacks->yajl_number = doc_handle_number;
	callbacks->yajl_string = doc_handle_string;
	callbacks->yajl_start_map = doc_handle_start_map;
	callbacks->yajl_map_key = doc_handle_map_key;
	callbacks->yajl_end_map = doc_handle_end_map;
	callbacks->yajl_start_array = doc_handle_start_array;
	callbacks->yajl_end_array = doc_handle_end_array;
	
	
	/* init column_data array */
	*column_data = (StringInfoData *) palloc(sizeof(StringInfoData) * col_size);
	
	/*
	 * Initialize YAJL Parser
	 */
	funcs = (yajl_alloc_funcs *) palloc(sizeof(yajl_alloc_funcs));
	funcs->malloc = palloc_wrapper;
	funcs->realloc = repalloc_wrapper;
	funcs->free = pfree_wrapper;
	funcs->ctx = (void *) &TopMemoryContext;
	
	
	/*
	 * json generator
	 */
	gen = yajl_gen_alloc(funcs);
	doc_gen = yajl_gen_alloc(funcs);
	
	/*
	 * parsing context
	 */
	ctx = (doc_context *) palloc(sizeof(doc_context));
	ctx->depth = 0;
	ctx->column_list = columns;
	ctx->column_data = *column_data;
	ctx->column_index = -1;
	ctx->num_of_columns = col_size;
	ctx->gen = gen;
	ctx->doc_gen = doc_gen;
	ctx->funcs = funcs;
	
	handle = yajl_alloc(callbacks, funcs, (void *) ctx);
	
	
	
	/*
	 * CURL the database url to get the database meta information
	 */
	curl = curl_easy_init();
	curl_easy_setopt(curl, CURLOPT_URL, doc_url.data);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, couchdbdoc_writer);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, &handle);
	curl_easy_perform(curl);
	curl_easy_cleanup(curl);
	
	/* if parsing is completed, free the parser */
	/*
	yajl_complete_parse(handle);
	yajl_gen_free(gen);
	yajl_free(handle);
	*/
	
#ifdef DEBUG
	elog(NOTICE, "columndata: '%s'", (*column_data)[0].data);
	elog(NOTICE, "columndata: '%s'", (*column_data)[1].data);
#endif
}

static size_t
couchdbdoc_writer(void *buffer, size_t size, size_t nmemb, void *userp)
{
	yajl_handle *handle = (yajl_handle *) userp;
	yajl_status stat = yajl_parse(*handle, (unsigned char *) buffer, size * nmemb);
	if (stat != yajl_status_ok) {
		unsigned char * str = yajl_get_error(*handle, 1, (unsigned char *) buffer, size * nmemb);
        elog(NOTICE, "%s", (char *) str);
		yajl_free_error(*handle, str);
	}
	return size * nmemb;
}


static int doc_handle_null(void * ctx)
{
	doc_context *context;
	yajl_gen doc_gen;
	
#ifdef DEBUG
	elog(NOTICE, "doc_handle_null");
#endif
	
	context = (doc_context *) ctx;
	
	/* for _doc attr if exits */
	doc_gen = context->doc_gen;
	yajl_gen_null(doc_gen);
	
	if (context->column_index == -1) {
		/* ignore such value */
		return 1;
	}
	else {
		if (context->depth == 1) {
			/* store this value directly in the column array */
			StringInfoData str;
			initStringInfo(&str);
			appendStringInfo(&str, "%s", "null");
			context->column_data[context->column_index] = str;
			return 1;
		}
		else {
			/* the value need to be pushed into a json structrue */
			yajl_gen gen;
			
			gen = (yajl_gen) context->gen;
			return yajl_gen_status_ok == yajl_gen_null(gen);
		}
	}
}

static int doc_handle_boolean(void * ctx, int boolean)
{
	doc_context *context;
	yajl_gen doc_gen;

#ifdef DEBUG
	elog(NOTICE, "doc_handle_boolean");
#endif
	
	context = (doc_context *) ctx;
	
	
	/* for _doc attr if exits */
	doc_gen = context->doc_gen;
	yajl_gen_bool(doc_gen, boolean);
	
	if (context->column_index == -1) {
		/* ignore such value */
		return 1;
	}
	else {
		if (context->depth == 1) {
			/* store this value directly in the column array */
			StringInfoData str;
			initStringInfo(&str);
			appendStringInfo(&str, "%s", boolean ? "true" : "false");
			context->column_data[context->column_index] = str;
			return 1;
		}
		else {
			/* the value need to be pushed into a json structrue */
			yajl_gen gen;
			
			gen = (yajl_gen) context->gen;
			return yajl_gen_status_ok == yajl_gen_bool(gen, boolean);
		}
	}
}

static int doc_handle_integer(void *ctx, long long integerVal)
{
#ifdef DEBUG
	elog(NOTICE, "doc_handle_integer");
#endif
	
	return 1;
}

static int doc_handle_double(void *ctx, double doubleVal)
{
#ifdef DEBUG
	elog(NOTICE, "doc_handle_double");
#endif
	return 1;
}

static int doc_handle_number(void * ctx, const char * s, size_t l)
{
	doc_context *context;
	yajl_gen doc_gen;
	
#ifdef DEBUG
	elog(NOTICE, "doc_handle_number");
#endif
	
	context = (doc_context *) ctx;
	
	/* for _doc attr if exits */
	doc_gen = context->doc_gen;
	yajl_gen_number(doc_gen, s, l);
	
	if (context->column_index == -1) {
		/* ignore such value */
		return 1;
	}
	else {
		if (context->depth == 1) {
			/* store this value directly in the column array */
			StringInfoData str;
			initStringInfo(&str);
			appendBinaryStringInfo(&str, s, (int) l);
			context->column_data[context->column_index] = str;
			return 1;
		}
		else {
			/* the value need to be pushed into a json structrue */
			yajl_gen gen;
			
			gen = (yajl_gen) context->gen;
			return yajl_gen_status_ok == yajl_gen_number(gen, s, l);
		}
	}
}

static int doc_handle_string(void * ctx, const unsigned char * stringVal,
							 size_t stringLen)
{	
	doc_context *context;
	yajl_gen doc_gen;
	
	context = (doc_context *) ctx;
	
#ifdef DEBUG
	elog(NOTICE, "doc_handle_string");
#endif
	
	/* for _doc attr if exits */
	doc_gen = context->doc_gen;
	yajl_gen_string(doc_gen, stringVal, stringLen);
	
	if (context->column_index == -1) {
		/* ignore such value */
		return 1;
	}
	else {
		if (context->depth == 1) {
			/* store this value directly in the column array */
			StringInfoData str;
			initStringInfo(&str);
			appendBinaryStringInfo(&str, (char *) stringVal, (int) stringLen);
			context->column_data[context->column_index] = str;
			
			return 1;
		}
		else {
			/* the value need to be pushed into a json structrue */
			yajl_gen gen;
			
			gen = (yajl_gen) context->gen;
			return yajl_gen_status_ok == yajl_gen_string(gen, stringVal, stringLen);
		}
	}
}

static int doc_handle_start_map (void *ctx)
{
	doc_context *context;
	yajl_gen doc_gen;
	
#ifdef DEBUG
	elog(NOTICE, "doc_handle_start_map");
#endif
	
	context = (doc_context *) ctx;
	(context->depth) ++;
	
	/* for _doc attr if exits */
	doc_gen = context->doc_gen;
	yajl_gen_map_open(doc_gen);
	
	if (context->column_index == -1) {
		/* ignore such value */
		return 1;
	}
	else {
		/* the value need to be pushed into a json structrue */
		yajl_gen gen;
		
		gen = (yajl_gen) context->gen;
		return yajl_gen_status_ok == yajl_gen_map_open(gen);
	}
}

static int doc_handle_map_key(void *ctx, const unsigned char * stringVal,
							  size_t stringLen)
{	
	doc_context *context;
	StringInfoData *column_list;
	StringInfoData map_key;
	yajl_gen doc_gen;
	
#ifdef DEBUG
	elog(NOTICE, "doc_handle_map_key");
#endif
	
	context = (doc_context *) ctx;
	initStringInfo(&map_key);
	appendBinaryStringInfo(&map_key, (char *) stringVal, stringLen);
	column_list = context->column_list;
	
	/* for _doc attr if exits */
	doc_gen = context->doc_gen;
	yajl_gen_string(doc_gen, stringVal, stringLen);
	
	if (context->depth == 1) {
		int i;
		
		/* re-init column_index as not-found */
		context->column_index = -1;
		/* if the current map key maps to a column, set the index */
		for (i = 0; i < context->num_of_columns; i++)
		{
			if (strcmp(column_list[i].data, map_key.data) == 0)
			{
				context->column_index = i;
			}
		}
	}
	else {
		if (context->column_index == -1) {
			return 1;
		}
		else {
			yajl_gen gen;
			
			gen = (yajl_gen) context->gen;
			return yajl_gen_status_ok == yajl_gen_string(gen, stringVal, stringLen);
		}
	}
	
	/* Save the map key to context, the following value parsed will belong to this key */
	context->map_key = map_key.data;
    return 1;
}

static int doc_handle_end_map(void * ctx)
{
	doc_context *context;
	yajl_gen doc_gen;
	
#ifdef DEBUG
	elog(NOTICE, "doc_handle_end_map");
#endif
	
	context = (doc_context *) ctx;
	(context->depth) --;
	
	/* for _doc attr if exits */
	doc_gen = context->doc_gen;
	yajl_gen_map_close(doc_gen);
	
	if (context->column_index == -1 || context->depth == 0) {
		if (context->depth == 0) {
			/* check if there a _doc mapping column which means the entire doc will be used as a column */
			int doc_index;
			bool has_doc = FALSE;
			const unsigned char *buf;
			size_t len;
			StringInfoData str;
			
			for (doc_index = 0; doc_index < context->num_of_columns; doc_index++)
			{
				if (strcmp(context->column_list[doc_index].data, "_doc") == 0)
				{
					has_doc = TRUE;
					break;
				}
			}
			
			/* if there is _doc column, we need to flush the doc_gen buffer */
			yajl_gen_get_buf(doc_gen, &buf, &len);
			initStringInfo(&str);
			appendBinaryStringInfo(&str, (char *) buf, (int) len);
			
			context->column_data[doc_index] = str;
			yajl_gen_clear(doc_gen);
			yajl_gen_free(doc_gen);
		}
		/* ignore such value */
		return 1;
	}
	else {
		/* the value need to be pushed into a json structrue */
		yajl_gen gen;
		yajl_gen_status status;
		
		gen = (yajl_gen) context->gen;
		status = yajl_gen_map_close(gen);
		
		/* flush buffer */
		if (context->depth == 1) {
			const unsigned char *buf;
            size_t len;
			StringInfoData str;
			
			
			yajl_gen_get_buf(gen, &buf, &len);
			initStringInfo(&str);
			appendBinaryStringInfo(&str, (char *) buf, (int) len);
			
			context->column_data[context->column_index] = str;
			yajl_gen_clear(gen);
			yajl_gen_free(gen);
			/* re-init gen */
			gen = yajl_gen_alloc(context->funcs);
		}
		return yajl_gen_status_ok == status;
	}
}

static int doc_handle_start_array(void * ctx)
{	
    doc_context *context;
	yajl_gen doc_gen;
	
#ifdef DEBUG
	elog(NOTICE, "doc_handle_start_array");
#endif
	
	context = (doc_context *) ctx;
	context->depth ++;
	
	/* for _doc attr if exits */
	doc_gen = context->doc_gen;
	yajl_gen_array_open(doc_gen);
	
	if (context->column_index == -1) {
		/* ignore such value */
		return 1;
	}
	else {
		/* the value need to be pushed into a json structrue */
		yajl_gen gen;
		
		gen = (yajl_gen) context->gen;
		return yajl_gen_status_ok == yajl_gen_array_open(gen);
	}
}

static int doc_handle_end_array(void * ctx)
{	
    doc_context *context;
	yajl_gen doc_gen;
	
#ifdef DEBUG
	elog(NOTICE, "doc_handle_end_array");
#endif
	
	context = (doc_context *) ctx;
	context->depth --;
	
	/* for _doc attr if exits */
	doc_gen = context->doc_gen;
	yajl_gen_array_close(doc_gen);
	
	if (context->column_index == -1 || context->depth == 0) {
		/* ignore such value */
		return 1;
	}
	else {
		/* the value need to be pushed into a json structrue */
		yajl_gen gen;
		yajl_gen_status status;
		
		gen = (yajl_gen) context->gen;
		status = yajl_gen_array_close(gen);
		
		/* flush buffer */
		if (context->depth == 1) {
			const unsigned char *buf;
            size_t len;
			StringInfoData str;
			
			
			yajl_gen_get_buf(gen, &buf, &len);
			initStringInfo(&str);
			appendBinaryStringInfo(&str, (char *) buf, (int) len);
			
			context->column_data[context->column_index] = str;
			
			yajl_gen_clear(gen);
			yajl_gen_free(gen);
			/* re-init gen */
			gen = yajl_gen_alloc(context->funcs);
		}
		return yajl_gen_status_ok == status;
	}
}



/*
 * function to retrieve a list of docs in couchdb
 */
static void
couchdbGetAllDocs(const char *address, const int port, const char *database, const char *username, const char *password,
				  const int limit, const char *startkey, const bool descending, StringInfoData *ids[], 
				  long long int *total_rows, long long int *offset)
{
	CURL	*curl;
	StringInfoData	alldocs_url;
	StringInfoData	auth_str;
	StringInfoData	startkey_str;
	StringInfoData	limit_str;
	alldocs_context *ctx;
	yajl_handle	handle;
	yajl_callbacks		*callbacks;
	yajl_alloc_funcs	*funcs;
	bool	hasAuth;
	bool	hasStartkey;
	bool	hasLimit;
	
#ifdef DEBUG
	elog(NOTICE, "couchdbGetAllDocs");
#endif
	
	/* Check if there is authen info specified */
	if (username != NULL && password != NULL) {
		initStringInfo(&auth_str);
		appendStringInfo(&auth_str, "%s:%s@", username, password);
		hasAuth = true;
	}
	else {
		hasAuth = false;
	}
	
	/* Check if there is startkey specified */
	if (startkey != NULL) {
		initStringInfo(&startkey_str);
		appendStringInfo(&startkey_str, "&startkey=\"%s\"", startkey);
		hasStartkey = true;
	}
	else {
		hasStartkey = false;
	}
	
	/* Check if there is limit specified */
	if (limit != -1) {
		initStringInfo(&limit_str);
		appendStringInfo(&limit_str, "&limit=%i", limit);
		hasLimit = true;
	}
	else {
		hasLimit = false;
	}
	
	
	/* Construct the URL we are going to curl */
	initStringInfo(&alldocs_url);
	appendStringInfo(&alldocs_url, "http://%s%s:%i/%s/_all_docs?descending=%s%s%s", 
					 hasAuth ? auth_str.data : "", address, port, database, descending ? "true" : "false", 
					 hasStartkey ? startkey_str.data : "", hasLimit ? limit_str.data : "");
	
	/* example: http://127.0.0.1:5984/cooldb/_all_docs?descending=false?startkey=123&limit=50 */
	
#ifdef DEBUG
	elog(NOTICE, "%s", alldocs_url.data);
#endif
	
	/* init array of strings to hold fetched ids */
	*ids = (StringInfoData *) palloc(sizeof(StringInfoData) * (hasLimit ? limit : 0));
	
	callbacks = (yajl_callbacks *) palloc(sizeof(yajl_callbacks));
	callbacks->yajl_null = NULL;
	callbacks->yajl_boolean = NULL;
	callbacks->yajl_integer = NULL;
	callbacks->yajl_double = NULL;
	callbacks->yajl_number = alldocs_handle_number;
	callbacks->yajl_string = alldocs_handle_string;
	callbacks->yajl_start_map = NULL;
	callbacks->yajl_map_key = alldocs_handle_map_key;
	callbacks->yajl_end_map = NULL;
	callbacks->yajl_start_array = NULL;
	callbacks->yajl_end_array = NULL;
	
	
	/*
	 * Initialize YAJL Parser
	 */
	funcs = (yajl_alloc_funcs *) palloc(sizeof(yajl_alloc_funcs));
	funcs->malloc = palloc_wrapper;
	funcs->realloc = repalloc_wrapper;
	funcs->free = pfree_wrapper;
	funcs->ctx = (void *) &TopMemoryContext;
	
	ctx = (alldocs_context *) palloc(sizeof(alldocs_context));
	ctx->total_rows = 0;
	ctx->offset = 0;
	ctx->counter = 0;
	ctx->ids = *ids;
	
	handle = yajl_alloc(callbacks, funcs, (void *) ctx);
	
	
	/*
	 * CURL the database url to get the database meta information
	 */
	curl = curl_easy_init();
	curl_easy_setopt(curl, CURLOPT_URL, alldocs_url.data);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, alldocs_writer);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, &handle);
	curl_easy_perform(curl);
	curl_easy_cleanup(curl);
	
	*total_rows = ctx->total_rows;
	*offset = ctx->offset;
}


static size_t
alldocs_writer(void *buffer, size_t size, size_t nmemb, void *userp)
{
	yajl_handle *handle = (yajl_handle *) userp;
	yajl_status stat = yajl_parse(*handle, (unsigned char *) buffer, size * nmemb);
	if (stat != yajl_status_ok) elog(NOTICE, "cannot parse");
	return size * nmemb;
}

static int alldocs_handle_number(void * ctx, const char * s, size_t l)
{	
	/* If the value's key is doc_count, store it into the id_buffer array  */
	alldocs_context *context;
	
#ifdef DEBUG
	elog(NOTICE, "alldocs_handle_number");
#endif
	
	context = (alldocs_context *) ctx;
	
	if ( strcmp("total_rows", context->map_key) == 0 ) {
		context->total_rows = (long long int) atoi(s);
	}
	else if ( strcmp("offset", context->map_key) == 0 ) {
		context->offset = (long long int) atoi(s);
	}
    return 1;
}

static int alldocs_handle_string(void * ctx, const unsigned char * stringVal,
								 size_t stringLen)
{	
	/* If the value's key is doc_count, store it into the id_buffer array  */
	alldocs_context	*context;
	StringInfoData	str;
	
#ifdef DEBUG
	elog(NOTICE, "alldocs_handle_string");
#endif
	
	context = (alldocs_context *) ctx;
	
	if ( strcmp("id", context->map_key) == 0 ) {
		initStringInfo(&str);
		appendBinaryStringInfo(&str, (char *) stringVal, (int) stringLen);
		context->ids[context->counter] = str;
		(context->counter) ++;
	}
    return 1;
}

static int alldocs_handle_map_key(void * ctx, const unsigned char * stringVal,
								  size_t stringLen)
{	
	StringInfoData map_key;
	alldocs_context * context;
	
#ifdef DEBUG
	elog(NOTICE, "alldocs_handle_map_key");
#endif
	
	initStringInfo(&map_key);
	appendBinaryStringInfo(&map_key, (char *) stringVal, (int) stringLen);
	
	/* Save the map key to context, the following value parsed will belong to this key */
	context = (alldocs_context *) ctx;
	context->map_key = map_key.data;
    return 1;
}



/*
 * get quals in the select if there is one
 */
static void
couchdbGetQual(Node *node, TupleDesc tupdesc, List *col_mapping_list, char **key, char **value, bool *equal)
{
	ListCell *col_mapping;
	*key = NULL;
	*value = NULL;
	*equal = false;
	
#ifdef DEBUG
	elog(NOTICE, "couchdbGetQual");
#endif
	
	if (!node)
		return;
	
	if (IsA(node, OpExpr))
	{
		OpExpr	*op = (OpExpr *) node;
		Node	*left, *right;
		Index	varattno;
		
		if (list_length(op->args) != 2)
			return;
		
		left = list_nth(op->args, 0);
		
		if (!IsA(left, Var))
			return;
		
		varattno = ((Var *) left)->varattno;
		
		right = list_nth(op->args, 1);
		
		if (IsA(right, Const))
		{
			StringInfoData  buf;
			
			initStringInfo(&buf);
			
			/* And get the column and value... */
			*key = NameStr(tupdesc->attrs[varattno - 1]->attname);
			*value = TextDatumGetCString(((Const *) right)->constvalue);
			
			
			/* convert qual keys to mapped couchdb attribute name */
			foreach(col_mapping, col_mapping_list)
			{
				DefElem *def = (DefElem *) lfirst(col_mapping);
				if (strcmp(def->defname, *key) == 0) {
					*key = defGetString(def);
					break;
				}
			}
			
			/*
			 * We can push down this qual if:
			 * - The operatory is TEXTEQ
			 * - The qual is on the _id column (in addition, _rev column can be also valid)
			 */
			
			if (op->opfuncid == PROCID_TEXTEQ)
				*equal = true;
#ifdef DEBUG
			elog(NOTICE, "Got qual %s = %s", *key, *value);
#endif
			return;
		}
	}
	
	return;
}