#ifndef FIXED_LIST_HH
#define FIXED_LIST_HH

template <typename T>
class less {
public:
    int operator () (T &a, T &b) {
        if (a < b) {
            return -1;
        } else if (b < a) {
            return 1;
        }
        return 0;
    }
};

template <typename T, typename Compare = less<T> >
class FixedList {
private:
    class ListNode {
    public:
        ListNode(T *_data = NULL) : next(NULL), prev(NULL), data(_data)  {}

        ListNode *next;
        ListNode *prev;
        T *data;
    } *head, *tail;

    void clear() {
        while (head != NULL) {
            ListNode *next = head->next;
            delete head;
            head = next;
        }
    }

    void insertBefore(ListNode *position, ListNode *p) {
        if (position == head) {
            head = p;
        } else {
            position->prev->next = p;
        }
        p->prev = position->prev;
        p->next = position;
        position->prev = p;
    }

    int _size;
    int _maxSize;
    Compare _comp;

public:
    class FixedListIterator {
    public:
        FixedListIterator(ListNode *node = NULL) : _node(node) {}

        T* operator *() {
            assert(_node && _node->data);
            return _node->data;
        }

        T* operator ++() {
            ListNode *old;
            do {
                old = _node;
                assert(old->next);
            } while (!ep_sync_bool_compare_and_swap(&_node, old, old->next));
            return old->data;
        }

        void swap(FixedListIterator &it) {
            ListNode *old;
            do {
                old = _node;
            } while (!ep_sync_bool_compare_and_swap(&_node, old, it._node));
        }
    private:
        ListNode *_node;
    };

    typedef FixedListIterator iterator;

    FixedList(int maxSize=-1, const Compare& comp = Compare())
        :   head(new ListNode),
            _size(0),
            _maxSize(maxSize),
            _comp(comp) {
        tail = head;
    }

    ~FixedList() {
        clear();
    }

    // Initialize the size
    void init(int maxSize) {
        clear();
        _maxSize = maxSize;
        _size = 0;
        head = new ListNode;
    }

    void insert(std::list<T> &l) {
        typename std::list<T>::iterator it;
        for (it = l.begin(); it != l.end(); it++) {
            insert(&(*it));
        }
    }

    int size() {
        return _size;
    }

    void insert(T *data) {
        ListNode *p = head;
        while (p != NULL) {
            if (p->next == NULL || _comp(*data, *p->data) <= 0) {
                insertBefore(p, new ListNode(data));
                break;
            }
            p = p->next;
        }
        if (_size == _maxSize) {
            while (p->next != NULL) {
                p = p->next;
            }
            ListNode *tmp = p->prev;
            p->prev = tmp->prev;
            if (p->prev == NULL) {
                head = p;
            } else {
                p->prev->next = p;
            }
            delete tmp;
        } else {
            _size++;
        }
    }

    iterator last() {
        assert(tail->prev);
        return iterator(tail->prev);
    }

    iterator begin() {
        return iterator(head);
    }
};

#endif // FIXED_LIST_HH
