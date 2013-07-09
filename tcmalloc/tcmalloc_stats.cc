/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

/*
 *   Copyright 2013 Zynga inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
#include "tcmalloc_stats.hh"

void TCMallocStats::getStats(std::map<std::string, size_t> &tc_stats) {
    size_t allocated_memory = 0;
    size_t heap_size = 0;
    size_t pageheap_free_bytes = 0;
    size_t pageheap_unmapped_bytes = 0;
    size_t max_thread_cache_bytes = 0;
    size_t current_thread_cache_bytes = 0;

    MallocExtension::instance()->GetNumericProperty("generic.current_allocated_bytes",
                                                    &allocated_memory);
    MallocExtension::instance()->GetNumericProperty("generic.heap_size",
                                                    &heap_size);
    MallocExtension::instance()->GetNumericProperty("tcmalloc.pageheap_free_bytes",
                                                    &pageheap_free_bytes);
    MallocExtension::instance()->GetNumericProperty("tcmalloc.pageheap_unmapped_bytes",
                                                    &pageheap_unmapped_bytes);
    MallocExtension::instance()->GetNumericProperty("tcmalloc.max_total_thread_cache_bytes",
                                                    &max_thread_cache_bytes);
    MallocExtension::instance()->GetNumericProperty("tcmalloc.current_total_thread_cache_bytes",
                                                    &current_thread_cache_bytes);

    tc_stats.insert(std::pair<std::string, size_t>("tcmalloc_allocated_bytes",
                                                   allocated_memory));
    tc_stats.insert(std::pair<std::string, size_t>("tcmalloc_heap_size",
                                                   heap_size));
    tc_stats.insert(std::pair<std::string, size_t>("tcmalloc_free_bytes",
                                                   pageheap_free_bytes));
    tc_stats.insert(std::pair<std::string, size_t>("tcmalloc_unmapped_bytes",
                                                   pageheap_unmapped_bytes));
    tc_stats.insert(std::pair<std::string, size_t>("tcmalloc_max_thread_cache_bytes",
                                                   max_thread_cache_bytes));
    tc_stats.insert(std::pair<std::string, size_t>("tcmalloc_current_thread_cache_bytes",
                                                   current_thread_cache_bytes));
}
