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

#include <sstream>

#include "pathexpand.hh"

#ifdef WIN32
const char* path_separator("\\/");
#else
const char* path_separator("/");
#endif

extern const char* path_separator;

static std::string pe_basename(const char *b) {
    assert(b);
    std::string s(b);
    size_t lastthing(s.find_last_of(path_separator));
    if (lastthing == s.npos) {
        return s;
    }

    return s.substr(lastthing + 1);
}

static std::string pe_dirname(const char *b) {
    assert(b);
    std::string s(b);
    size_t lastthing(s.find_last_of(path_separator));
    if (lastthing == s.npos) {
        return std::string(".");
    }

    return s.substr(0, lastthing);
}

PathExpander::PathExpander(const char *p) : dir(pe_dirname(p)),
    base(pe_basename(p)) {
}

std::string PathExpander::expand(const char *pattern, int shardId) {
    std::stringstream ss;

    while (*pattern) {
        if (*pattern == '%') {
            ++pattern;
            switch (*pattern) {
            case 'd':
                ss << dir;
                break;
            case 'b':
                ss << base;
                break;
            case 'i':
                ss << shardId;
                break;
            default:
                ss << '%' << *pattern;
            }
        } else {
            ss << *pattern;
        }
        ++pattern;
    }

    return ss.str();
}
