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
#include "config.h"
#include "priority.hh"
#undef NDEBUG
#include <assert.h>

int main(int argc, char **argv) {
   (void)argc; (void)argv;

   assert(Priority::BgFetcherPriority > Priority::TapBgFetcherPriority);
   assert(Priority::TapBgFetcherPriority == Priority::VBucketPersistHighPriority);
   assert(Priority::VBucketPersistHighPriority > Priority::VKeyStatBgFetcherPriority);
   assert(Priority::VKeyStatBgFetcherPriority > Priority::NotifyVBStateChangePriority);
   assert(Priority::NotifyVBStateChangePriority > Priority::FlusherPriority);
   assert(Priority::FlusherPriority > Priority::ItemPagerPriority);
   assert(Priority::ItemPagerPriority > Priority::VBucketDeletionPriority);

   return 0;
}
