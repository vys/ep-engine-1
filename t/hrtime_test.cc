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
#include "common.hh"
#undef NDEBUG
#include <assert.h>

int main(int argc, char **argv) {
   (void)argc; (void)argv;

   assert(hrtime2text(0) == "0 usec");
   assert(hrtime2text(9999) == "9999 usec");
   assert(hrtime2text(10000) == "10 ms");
   assert(hrtime2text(9999999) == "9999 ms");
   assert(hrtime2text(10000000) == "10 s");

   // Using math for some of the bigger ones because compilers on 32
   // bit systems whine about large integer constants.
   hrtime_t val = 10000;
   val *= 1000000;
   assert(hrtime2text(val) == "10000 s");
   assert(hrtime2text(val - 1) == "9999 s");

   hrtime_t now = gethrtime();
   usleep(200);
   hrtime_t later = gethrtime();
   assert(now + 200 < later);

   return 0;
}
