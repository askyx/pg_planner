extern "C" {
#include <postgres.h>
#include <fmgr.h>

#include <commands/explain.h>
#include <optimizer/planner.h>
#include <utils/guc.h>
}

namespace pgp {

void DefinePlannerGuc();
void DefinePlannerHooks();

}  // namespace pgp