#ifndef EP_TESTSUITE_H
#define EP_TESTSUITE_H 1

#include <memcached/engine.h>
#include <memcached/engine_testapp.h>

#ifdef __cplusplus
extern "C" {
#endif

MEMCACHED_PUBLIC_API
engine_test_t* get_tests(void);

MEMCACHED_PUBLIC_API
bool setup_suite(struct test_harness *th);

MEMCACHED_PUBLIC_API
bool teardown_suite();

enum test_result prepare(engine_test_t *test);

void cleanup(engine_test_t *test, enum test_result result);

/**
 * Base class for key generator
 */
class BaseLoadPattern {
public:
    uint32_t curr;
    uint32_t nkeys;
    uint64_t timeout;
    uint64_t start_time;
    uint32_t ops_max;
    uint32_t ops_count;

    int getNextKey(std::string &key) {
        if (!start_time) {
            start_time = time(NULL);
        }

        if (timeout && (time(NULL) - start_time) > timeout) {
            return -1;
        }

        if (ops_max && ops_count == ops_max) {
            return -2;
        }

        int rv = generateKey(key);
        if (rv) {
            ops_count++;
        }
        return rv;
    }

    /**
     * Method which generate next key
     * returns > 0 if success
     */
    virtual int generateKey(std::string &key) = 0;

    /**
     * @nk - number of keys
     * @tm - time out in seconds
     * @mxops - maximum operations limit
     */
    BaseLoadPattern(uint32_t nk, uint64_t tm, uint32_t mxops) : curr(0), nkeys(nk), timeout(tm), start_time(0), ops_max(mxops), ops_count(0) {}
    virtual ~BaseLoadPattern() {};

};

class EvenKeysPattern: public BaseLoadPattern {

    int generateKey(std::string &key) {
        bool found = false;
        while (curr < nkeys) {
            if (curr % 2 == 0) {
                std::stringstream ss;
                ss<<"key-"<<curr;
                key = ss.str();
                found = true;
            }
            curr++;
            if ((timeout || ops_max) && curr >= nkeys - 1) {
                curr = 0;
            }

            if (found) {
                return 1;
            }
        }
        return 0;

    }

public:
    EvenKeysPattern(uint32_t n, uint64_t t = 0, uint32_t ops = 0) : BaseLoadPattern(n, t, ops) {
        std::cout<<"Loading even keylist pattern, nkeys="<<n<<" timeout="<<t<<" maxops="<<ops<<std::endl;
    }
};


#ifdef __cplusplus
}

#endif

#endif
