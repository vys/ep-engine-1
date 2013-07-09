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
#ifndef SQLITE_EVAL_HH
#define SQLITE_EVAL_HH 1

#include <cassert>

#ifdef USE_SYSTEM_LIBSQLITE3
#include <sqlite3.h>
#else
#include "embedded/sqlite3.h"
#endif

/**
 * Evaluates SQLite files within a given DB.
 */
class SqliteEvaluator {
public:

    SqliteEvaluator(sqlite3 *d) : db(d) {
        assert(db);
    }

    void eval(const std::string &filename);

private:

    void execute(std::string &query);
    void trim(std::string &str);

    sqlite3 *db;
};

#endif /* SQLITE_EVAL_HH */
