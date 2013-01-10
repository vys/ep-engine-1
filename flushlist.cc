#include "flushlist.hh"
#include "ep.hh"

void FlushLists::get(FlushList& out, int kvId) {
    FlushList shardList;
    int nShards = epStore->getRWUnderlying(kvId)->getNumShards();
    for (int i = 0; i < nShards; i++) {
        flushLists[kvId*maxShards+i].getAll(shardList); // This will get all per-thread caches into a single list from the AtomicList specific to this shard.
        epStore->getRWUnderlying(kvId)->optimizeWrites(shardList); // This will rearrange the elements in shardList in an order that is optimized for writes. Because it is a list::sort internally, it should not involve any copy/ctor/dtors.
        out.splice(out.end(), shardList); // This should move all elements from shardList to the end of out list, leaving shardList empty for next run.
    }

}

