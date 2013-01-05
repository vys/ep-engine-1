#include <cassert>
#include "config.h"
#include <fixed_list.hh>
#include <iostream>
#include <vector>
#include <algorithm>

#define TOTAL_SIZE 5000000
#define LIST_SIZE   500000

using namespace std;

class Item {
public:
    Item(int _a) : a(_a) {}

    int operator< (const Item& b) { // this is enough for default comparator in FixedList
        return a < b.a;
    }

    int a;
};

void destructItems(FixedList<Item> &l) {
    FixedList<Item>::iterator it = l.begin();
    while (it != l.end()) {
        delete it++;
    }
}

void populate_list(FixedList<Item> &l, vector<int> &v) {
    for (int i = 0; i < TOTAL_SIZE; i++) {
        l.insert(new Item(v[i]));
    }
}

void populate_vector(vector<int> &v) {
    srand(time(NULL));
#ifdef PRINT_MODE
    cout << "Original:\n";
#endif
    for (int i = 0; i < TOTAL_SIZE; i++) {
        int c = rand();
#ifdef PRINT_MODE
        cout << c << endl;
#endif
        v.push_back(c);
    }
}

#ifdef PRINT_MODE
void printAll(FixedList<Item> &l, vector<int> &v) {
    FixedList<Item>::iterator it = l.begin();
    cout << "FixedList:\n";
    while (it != l.end()) {
        Item *i = it++;
        cout << i->a << endl;
    }
    cout << "Vector:\n";
    int vs = v.size();
    for (int i = 0; i < vs; i++)
        cout << v[i] << endl;
}
#endif

void checkNumLessThanVX(FixedList<Item> &l, vector<int> &v, int x) {
    Item dummy(0);
    if (x == LIST_SIZE) {
        dummy.a = v[x-1] + 1;
    } else {
        while (x > 0 && v[x-1] == v[x]) {
            x--;
        }
        dummy.a = v[x];
    }
    assert(l.numLessThan(&dummy) == x);
}

// ----------------------------------------------------------------------
// Actual tests below.
// ----------------------------------------------------------------------

void checkSort(FixedList<Item> &l, vector<int> &v) {
    l.checkSanity();
    FixedList<Item>::iterator it = l.begin();
    size_t count = 0;
    while (it != l.end()) {
        Item *i = it++;
        assert(count < v.size());
        assert(i->a == v[count]);
        count++;
    }
    assert(count == LIST_SIZE);
}

void checkPreBuildSanity(FixedList<Item> &l) {
    l.checkSanity();
}

void checkNumLessThan(FixedList<Item> &l, vector<int> &v) {
    int tries = 10;
    checkNumLessThanVX(l, v, 0);
    checkNumLessThanVX(l, v, LIST_SIZE);
    for (int i = 0; i < tries; i++) {
        checkNumLessThanVX(l, v, rand() % LIST_SIZE);
    }
}

int main() {
    vector<int> v;
    FixedList<Item> l(LIST_SIZE);
    populate_vector(v);

#ifdef TIMED_MODE
    time_t start, end;
    time(&start);
#endif

    populate_list(l, v);

#ifndef TIMED_MODE
    checkPreBuildSanity(l);
#endif

    l.build();

#ifdef TIMED_MODE
    time(&end);
    cout << "Time spent in seconds: " << difftime(end, start) << endl;
#endif

    std::sort(v.begin(), v.end());

    checkNumLessThan(l, v);

#ifdef PRINT_MODE
    printAll(l, v);
#endif

    checkSort(l, v);

    destructItems(l);
}
