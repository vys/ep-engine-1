#!/usr/bin/env python

#   Copyright 2013 Zynga inc.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

import glob

def expand_file_pattern(file_pattern):
    """Returns an unused filename based on a file pattern,
       like "/some/where/backup-%.mbb", replacing the optional
       placeholder character ('%') with the next, unused, zero-padded number.
       Example return value would be "/some/where/backup-00000.mbb".
    """
    not_hash = file_pattern.split('%')
    if len(not_hash) == 1:
        return file_pattern
    if len(not_hash) != 2:
        raise Exception("file pattern should have"
                        + " at most one '%' placeholder character: "
                        + file_pattern)

    max = -1
    existing = glob.glob(file_pattern.replace('%', '*'))
    for e in existing:
        for s in not_hash:
            e = e.replace(s, '')
        n = int(e)
        if max < n:
            max = n

    return file_pattern.replace('%', str(max + 1).zfill(5))


def retrieve_missing_file_seq_numbers(files):
    """Return any missing file seq numbers by checking sequence numbers used in
      the names of input files. For example, if the input files are
      ['backup-00001.mbb', 'backup-00003.mbb', 'backup-00004.mbb'],
      then, it returns ['0002']. This function assumes that a sequence number is
      followed by '.' + a file's extension.
    """

    if len(files) == 1:
        return []

    files.sort()
    curr_seq = -1
    missing_seq_nums = []
    for file in files:
        tokens = file.split('-')
        seq_token = tokens[len(tokens) - 1]
        next_seq = int(seq_token.split('.')[0])
        if curr_seq != -1:
            if curr_seq + 1 != next_seq:
                mseq = str(curr_seq + 1).zfill(5)
                missing_seq_nums.append(mseq)
        curr_seq = next_seq

    return missing_seq_nums
