// If you choose to use C++, read this very carefully:
// https://www.postgresql.org/docs/15/xfunc-c.html#EXTEND-CPP

#include "dog.h"

// clang-format off
extern "C" {
#include "../../../../src/include/postgres.h"
#include "../../../../src/include/fmgr.h"
#include "../../../../src/include/foreign/fdwapi.h"
}
// clang-format on

// Obtain relation size estimates for a foreign table.
extern "C" void db721_GetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel,
                                      Oid foreigntableid) {
  // TODO(721): Write me!
  Dog terrier("Terrier");
  elog(LOG, "db721_GetForeignRelSize: %s", terrier.Bark().c_str());
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
}

// Create a ForeignScan plan node from the selected foreign access path. 
// This is called at the end of query planning. 
extern "C" ForeignScan *
db721_GetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid,
                   ForeignPath *best_path, List *tlist, List *scan_clauses,
                   Plan *outer_plan) {
  // TODO(721): Write me!
  return nullptr;
}

// Begin executing a foreign scan. This is called during executor startup.
// It should perform any initialization needed before the scan can start, 
// but not start executing the actual scan (that should be done upon the 
// first call to IterateForeignScan). 
extern "C" void db721_BeginForeignScan(ForeignScanState *node, int eflags) {
  // TODO(721): Write me!
}

// Fetch one row from the foreign source, returning it in a tuple table slot 
// (the node's ScanTupleSlot should be used for this purpose). Return NULL if no more rows are available. 
extern "C" TupleTableSlot *db721_IterateForeignScan(ForeignScanState *node) {
  // TODO(721): Write me!
  return nullptr;
}

// Restart the scan from the beginning. 
// Note that any parameters the scan depends on may have changed value, 
// so the new scan does not necessarily return exactly the same rows.
extern "C" void db721_ReScanForeignScan(ForeignScanState *node) {
  // TODO(721): Write me!
}

// End the scan and release resources.
// It is normally not important to release palloc'd memory, 
// but for example open files and connections to remote servers should be cleaned up.
extern "C" void db721_EndForeignScan(ForeignScanState *node) {
  // TODO(721): Write me!
}