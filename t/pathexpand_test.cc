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

#include <string>
#include <iostream>
#include <cstdlib>

#include "pathexpand.hh"

#define assertEquals(a, b) _assertEquals(a, b, __LINE__)

static void _assertEquals(const char *e, const std::string &g, int lineno) {
    std::string s(e);
    if (e != g) {
        std::cerr << "Expected ``" << e << "'' got ``" << g << "''"
                  << " on line " << lineno << std::endl;
        abort();
    }
}

static void testFullPath() {
    PathExpander p("/some/path/to/maindb");

    assertEquals("/some/path/to", p.expand("%d", 0));
    assertEquals("maindb", p.expand("%b", 0));
    assertEquals("13", p.expand("%i", 13));
    assertEquals("/some/path/to/maindb-3.sqlite",
                 p.expand("%d/%b-%i.sqlite", 3));
    assertEquals("/some/path/to/maindb-3.mb",
                 p.expand("%d/%b-%i.mb", 3));
    assertEquals("/some/path/to/maindb-data/maindb-3.mb",
                 p.expand("%d/%b-data/%b-%i.mb", 3));
}

static void testRelativePath() {
    PathExpander p("maindb");

    assertEquals(".", p.expand("%d", 0));
    assertEquals("maindb", p.expand("%b", 0));
    assertEquals("13", p.expand("%i", 13));
    assertEquals("./maindb-3.sqlite", p.expand("%d/%b-%i.sqlite", 3));
}

int main() {

    testFullPath();
    testRelativePath();

    return 0;
}
