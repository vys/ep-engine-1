#include "flushlist.hh"
#include "ep.hh"

void FlushLists::get(FlushList& out, int kvId) {
    int nShards = epStore->getRWUnderlying(kvId)->getNumShards();
    for (int i = 0; i < nShards; i++) {
        flushLists[kvId*maxShards+i].getAll(shardList[kvId]); // This will get all per-thread caches into a single list from the AtomicList specific to this shard.
        epStore->getRWUnderlying(kvId)->optimizeWrites(shardList[kvId]); // This will rearrange the elements in shardList in an order that is optimized for writes. Because it is a list::sort internally, it should not involve any copy/ctor/dtors.
        out.splice(out.end(), shardList[kvId]); // This should move all elements from shardList to the end of out list, leaving shardList empty for next run.
    }

}

// Does not interfere with get() because this is called only when the
// flusher is paused. Hence no locks required.
void FlushLists::getCopy(std::list<queued_item> &out, int kvId) {
    int nShards = epStore->getRWUnderlying(kvId)->getNumShards();
    for (int i = 0; i < nShards; i++) {
        flushLists[kvId*maxShards+i].getAll(shardList[kvId]); // This will get all per-thread caches into a single list from the AtomicList specific to this shard.
        epStore->getRWUnderlying(kvId)->optimizeWrites(shardList[kvId]); // This will rearrange the elements in shardList in an order that is optimized for writes. Because it is a list::sort internally, it should not involve any copy/ctor/dtors.            
    }

    epStore->copyItemsFromFlushList(shardList[kvId], out);
}
