#include <cassert>
#include "config.h"
#include <fixed_list.hh>
#include <iostream>
#include <vector>

#define TOTAL_SIZE 50000000
#define LIST_SIZE   500000
#define PRINT_TIME

using namespace std;

double total_time;

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
    time_t start, end;
    time(&start);
    for (int i = 0; i < TOTAL_SIZE; i++) {
        l.insert(new Item(v[i]));
    }
    time(&end);
    total_time += difftime(end, start);
}

void populate(FixedList<Item> &l, vector<int> &v) {
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
    populate_list(l, v);
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

int main() {
    total_time = 0;
    vector<int> v;
    FixedList<Item> l(LIST_SIZE);
    populate(l, v);

    checkPreBuildSanity(l);

    time_t start, end;
    time(&start);
    l.build();
    time(&end);
    total_time += difftime(end, start);

    sort(v.begin(), v.end());

#ifdef PRINT_MODE
    printAll(l, v);
#endif

    checkSort(l, v);

    destructItems(l);

#ifdef PRINT_TIME
    cout << "Time spent in seconds: " << total_time << endl;
#endif
}
