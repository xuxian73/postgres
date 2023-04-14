// If you choose to use C++, read this very carefully:
// https://www.postgresql.org/docs/15/xfunc-c.html#EXTEND-CPP

#include "dog.h"
#include "parser.h"
// clang-format off
extern "C" {
#include "../../../../src/include/postgres.h"
#include "../../../../src/include/fmgr.h"
#include "../../../../src/include/foreign/fdwapi.h"
#include "../../../../src/include/foreign/foreign.h"
#include "../../../../src/include/commands/defrem.h"
#include "../../../../src/include/optimizer/pathnode.h"
#include "../../../../src/include/optimizer/planmain.h"
#include "../../../../src/include/optimizer/optimizer.h"
#include "../../../../src/include/executor/executor.h"
#include "../../../../src/include/utils/rel.h"
#include "../../../../src/include/access/table.h"
#include "../../../../src/include/nodes/makefuncs.h"
}
// clang-format on

typedef struct db721FdwPlanState 
{
  char * filename;
  char * tablename;
} db721FdwPlanState;

typedef struct db721FdwExecState
{
  char * filename;
  char * tablename;
  db721_Parser parser;
} db721FdwExecState;

bool getWhereClause(RelOptInfo *baserel, Oid foreigntableid, List **columns) {
  Bitmapset *attrs_used = NULL;
  ListCell *lc;
  Relation rel;
  TupleDesc tupleDesc;
  AttrNumber attnum;
  bool has_wholerow = false;
  pull_varattnos((Node *) baserel->reltarget->exprs, baserel->relid,
				   &attrs_used);
  elog(LOG, "target expres: %s", nodeToString(baserel->reltarget->exprs));
  foreach(lc, baserel->baserestrictinfo) {
    RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);
    elog(LOG, "restrict info: %s", nodeToString(rinfo));
    elog(LOG, "where clause: %s", nodeToString(rinfo->clause));
    if (rinfo->pseudoconstant)
      continue;
    pull_varattnos((Node *) rinfo->clause, baserel->relid, &attrs_used);
  }
  /* Convert attribute numbers to column names. */
	rel = table_open(foreigntableid, AccessShareLock);
	tupleDesc = RelationGetDescr(rel);

	while ((attnum = bms_first_member(attrs_used)) >= 0)
	{
		/* Adjust for system attributes. */
		attnum += FirstLowInvalidHeapAttributeNumber;

		if (attnum == 0)
		{
			has_wholerow = true;
			break;
		}

		/* Ignore system attributes. */
		if (attnum < 0)
			continue;

		/* Get user attributes. */
		if (attnum > 0)
		{
			Form_pg_attribute attr = TupleDescAttr(tupleDesc, attnum - 1);
			char	   *attname = NameStr(attr->attname);
			/*
			 * Skip generated columns (COPY won't accept them in the column
			 * list)
			 */
			if (attr->attgenerated)
				continue;
			*columns = lappend(*columns, makeString(pstrdup(attname)));
		}
	}
  table_close(rel, AccessShareLock);
  if (has_wholerow) {
    *columns = NIL;
    return false;
  }
  return true;
}

// Obtain relation size estimates for a foreign table.
extern "C" void db721_GetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel,
                                      Oid foreigntableid) {
  elog(LOG, "db721_GetForeignRelSize"); 
  db721FdwPlanState *fdw_private = (db721FdwPlanState *)palloc0(sizeof(db721FdwPlanState));
  // Get table Name of Relation
  ForeignTable* table;
  table = GetForeignTable(foreigntableid);
  List* options = table->options;
  ListCell* cell;
  foreach(cell, options)
  {
    DefElem* def = (DefElem*)lfirst(cell);
    if (strcmp(def->defname, "filename") == 0)
    {
      fdw_private->filename = defGetString(def);
    }
    if (strcmp(def->defname, "tablename") == 0)
    {
      fdw_private->tablename = defGetString(def);
    }
  }
  baserel->fdw_private = (void *)fdw_private;
  elog(LOG, "db721_GetForeignRelSize: %s", fdw_private->filename);
  elog(LOG, "db721_GetForeignRelSize: %s", fdw_private->tablename);
}

// Create possible access paths for a scan on a foreign table. 
// This is called during query planning.
// This function must generate at least one access path (ForeignPath node) 
// for a scan on the foreign table and must call add_path to add each such 
// path to baserel->pathlist. It's recommended to use create_foreignscan_path 
// to build the ForeignPath nodes. The function can generate multiple access 
// paths, e.g., a path which has valid pathkeys to represent a pre-sorted result. 
// Each access path must contain cost estimates, and can contain any FDW-private 
// information that is needed to identify the specific scan method intended.
extern "C" void db721_GetForeignPaths(PlannerInfo *root, RelOptInfo *baserel,
                                    Oid foreigntableid) {
  // TODO(721): Write me!
  Dog scout("Scout");
  elog(LOG, "db721_GetForeignPaths: %s", scout.Bark().c_str());
  db721FdwPlanState *fdw_private = (db721FdwPlanState *) baserel->fdw_private;
  Cost startup_cost = 0.0;
  Cost total_cost = 0.0;
  // List *columns;
  // List *whereClauseCol = NIL;
  // if (getWhereClause(baserel, foreigntableid, &whereClauseCol)) {
  //    whereClauseCol = list_make1(makeDefElem("whereClause", (Node *) columns, -1));
  // }
  add_path(baserel, (Path *)
    create_foreignscan_path(root, 
                            baserel, 
                            NULL, /* default pathtarget */
                            baserel->rows, 
                            startup_cost, 
                            total_cost, 
                            NIL, /* no pathkeys */
                            baserel->lateral_relids, 
                            NULL, /* no extra plan */
                            NIL));
  elog(LOG, "db721_GetForeignPaths, %s", fdw_private->filename);
}

List *
extract_actual_clauses(List *restrictinfo_list, bool pseudoconstant)
{
	List	   *result = NIL;
	ListCell   *l;

	foreach(l, restrictinfo_list)
	{
		RestrictInfo *rinfo = lfirst_node(RestrictInfo, l);
    elog(LOG, "extract_actual_clauses: %s", nodeToString(rinfo->clause));
    if (rinfo->pseudoconstant == pseudoconstant)
		  result = lappend(result, rinfo->clause);
  }
	return result;
}

// Create a ForeignScan plan node from the selected foreign access path. 
// This is called at the end of query planning. 
extern "C" ForeignScan *
db721_GetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid,
                   ForeignPath *best_path, List *tlist, List *scan_clauses,
                   Plan *outer_plan) {
  // TODO(721): Write me!
  Dog rover("Rover");
  elog(LOG, "db721_GetForeignPlan: %s", rover.Bark().c_str());
  // db721FdwPlanState *db721_plan_state = best_path->fdw_private->elements[0].ptr_value;
  scan_clauses = extract_actual_clauses(scan_clauses, false);
  return make_foreignscan(tlist, 
                          scan_clauses, 
                          baserel->relid, 
                          NIL, 
                          best_path->fdw_private, 
                          NIL, 
                          NIL, 
                          outer_plan);
}

// Begin executing a foreign scan. This is called during executor startup.
// It should perform any initialization needed before the scan can start, 
// but not start executing the actual scan (that should be done upon the 
// first call to IterateForeignScan). 
extern "C" void db721_BeginForeignScan(ForeignScanState *node, int eflags) {
  elog(LOG, "db721_BeginForeignScan");
  // ForeignScan *db721_plan = (ForeignScan *)node->ss.ps.plan;
  
  db721FdwExecState *fdw_state = (db721FdwExecState *)palloc0(sizeof(db721FdwExecState));
  ForeignTable* table;
  table = GetForeignTable(RelationGetRelid(node->ss.ss_currentRelation));
  List* options = table->options;
  ListCell* cell;
  foreach(cell, options)
  {
    DefElem* def = (DefElem*)lfirst(cell);
    if (strcmp(def->defname, "filename") == 0)
    {
      fdw_state->filename = defGetString(def);
    }
    if (strcmp(def->defname, "tablename") == 0)
    {
      fdw_state->tablename = defGetString(def);
    }
  }
  if (eflags & EXEC_FLAG_EXPLAIN_ONLY) {
    return;
  }
  fdw_state->parser.init(fdw_state->filename);
  node->fdw_state = (void *)fdw_state;
  elog(LOG, "db721_BeginForeignScan: %s", fdw_state->filename);
}

// Fetch one row from the foreign source, returning it in a tuple table slot 
// (the node's ScanTupleSlot should be used for this purpose). Return NULL if no more rows are available. 
extern "C" TupleTableSlot *db721_IterateForeignScan(ForeignScanState *node) {
  // TODO(721): Write me!
  db721FdwExecState *fdw_state = (db721FdwExecState *)node->fdw_state;
  TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
  ExecClearTuple(slot);
  if (fdw_state->parser.next(slot)) {
    ExecStoreVirtualTuple(slot);
  }
  return slot;
}

// Restart the scan from the beginning. 
// Note that any parameters the scan depends on may have changed value, 
// so the new scan does not necessarily return exactly the same rows.
extern "C" void db721_ReScanForeignScan(ForeignScanState *node) {
  // TODO(721): Write me!
  db721FdwExecState *fdw_state = (db721FdwExecState *)node->fdw_state;
  fdw_state->parser.reset();
  elog(LOG, "db721_ReScanForeignScan: %s", fdw_state->filename);
}

// End the scan and release resources.
// It is normally not important to release palloc'd memory, 
// but for example open files and connections to remote servers should be cleaned up.
extern "C" void db721_EndForeignScan(ForeignScanState *node) {
  // TODO(721): Write me!
  db721FdwExecState *fdw_state = (db721FdwExecState *)node->fdw_state;
  fdw_state->parser.close();
  pfree(fdw_state->filename);
  pfree(fdw_state->tablename);
  elog(LOG, "db721_EndForeignScan: %s", fdw_state->filename);
  pfree(fdw_state);
}