#include <signal.h>
#include <unistd.h>

#include <cassert>
#include <set>

#include <ep.hh>
#include <evict.hh>
#include <item.hh>
#include <stats.hh>
#include <stored-value.hh>

#define EXPIRYINF      100

using namespace std;

time_t time_offset;

extern "C" {
    static rel_time_t basic_current_time(void) {
        return 0;
    }

    rel_time_t (*ep_current_time)() = basic_current_time;

    time_t ep_real_time() {
        return time(NULL) + time_offset;
    }
}

EPStats global_stats;
HashTable global_ht(global_stats);
StoredValueFactory global_svFactory(global_stats);
set<string> keylist;

string gen_random_string(int len=15) {
    string s(len, ' ');
    for (int i = 0; i < len; ++i) {
        int randomChar = rand()%(26+26+10);
        if (randomChar < 26)
            s[i] = 'a' + randomChar;
        else if (randomChar < 26+26)
            s[i] = 'A' + randomChar - 26;
        else
            s[i] = '0' + randomChar - 26 - 26;
    }
    return s;
}

static StoredValue *generateStoredValue() {
    string key;
    do {
        key = gen_random_string();
    } while (keylist.find(key) != keylist.end());
    keylist.insert(key);
    string data = gen_random_string(50);
    Item itm(key, 0, EXPIRYINF, data.c_str(), data.size());
    return global_svFactory(itm, NULL, global_ht);
}

// ----------------------------------------------------------------------
// Actual tests below.
// ----------------------------------------------------------------------

static void testNewList() {
    /*
    keylist.clear();
    lruList *l = new lruList(NULL, global_stats);
    l->clearLRU(l);
    StoredValue *s = generateStoredValue();
    cout << s->isDirty() << endl;
    */
}

int main() {
    alarm(60);
    srand(time(NULL));
    testNewList();
    exit(0);
}
