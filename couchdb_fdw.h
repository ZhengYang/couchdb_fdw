/*----------------------------------------------------------
 *
 *          foreign-data wrapper for CouchDB
 *
 * Copyright (c) 2011, PostgreSQL Global Development Group
 *
 * This software is released under the PostgreSQL Licence.
 *
 * Author: Zheng Yang <zhengyang4k@gmail.com>
 *
 * IDENTIFICATION
 *        couchdb_fdw/couchdb_fdw.h
 *
 *----------------------------------------------------------
 */

/*----------------------------------------------------------
 *
 * Ported to PostgreSQL 9.2+ Foreign Data Wrapper API
 *
 * Author: Gauthier Boabglio <gauthier.boaglio _4t_ gmail _D0t_ com>
 *
 *----------------------------------------------------------
 */

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
typedef struct context
{
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

typedef struct dbsize_context
{
    long int doc_count;
    char *map_key;
} dbsize_context;

typedef struct alldocs_context
{
    char *map_key;
    long long int total_rows;
    long long int offset;
    int counter;
    StringInfoData *ids;
} alldocs_context;

typedef struct doc_context
{
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

typedef struct couch_doc
{
    char *doc_id;
    char *doc_rev;
    char *doc;
} couch_doc;

typedef struct curl_data
{
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
 * FDW callback routines
 */
// New 9.2 Api
#if (PG_VERSION_NUM >= 90200)
	static void couchdbGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);
	static void couchdbGetForeignPaths(PlannerInfo *root,RelOptInfo *baserel,Oid foreigntableid);
	static bool couchdbAnalyzeForeignTable(Relation relation,AcquireSampleRowsFunc *func,BlockNumber *totalpages);
	static ForeignScan * couchdbGetForeignPlan(PlannerInfo *root,RelOptInfo *baserel,Oid foreigntableid, ForeignPath *best_path,List * tlist, List *scan_clauses, Plan *outer_plan);
	// New
	static void estimate_costs(PlannerInfo *root,RelOptInfo *baserel,Cost *startup_cost,Cost *total_cost,Oid foreigntableid);
#else
	static FdwPlan *couchdbPlanForeignScan(Oid foreigntableid, PlannerInfo *root, RelOptInfo *baserel);
#endif

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
