/*********************************************************************************
 * Modifications Copyright 2017-2019 eBay Inc.
 *
 * Author/Developer(s): Harihara Kadayam
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 *********************************************************************************/
#pragma once

#include <boost/intrusive/slist.hpp>
#include <boost/functional/hash.hpp>
#include <shared_mutex>

#include <sisl/fds/utils.hpp>
#include <sisl/cache/cache_common.hpp>
#include <sisl/cache/hash_entry_base.hpp>

namespace sisl {

// Convenience type aliases — kept for call-site use; the class itself is now templated.
template < typename K, typename V >
using key_access_cb_t = std::function< void(const ValueEntryBase&, const K&, const hash_op_t) >;

template < typename K, typename V >
using key_extractor_cb_t = std::function< K(const V&) >;

static constexpr size_t s_start_seed = 0; // TODO: Pickup a better seed

template < typename K, typename V, typename KeyExtractor, typename AccessCb >
class SimpleHashBucket;

///////////////////////////////////////////// SimpleHashMap Declaration ///////////////////////////////////
template < typename K, typename V, typename KeyExtractor, typename AccessCb = NullCacheCb >
class SimpleHashMap {
private:
    uint32_t m_nbuckets;
    SimpleHashBucket< K, V, KeyExtractor, AccessCb >* m_buckets;
    KeyExtractor m_key_extract_cb;
    [[no_unique_address]] AccessCb m_key_access_cb;

    static thread_local SimpleHashMap< K, V, KeyExtractor, AccessCb >* s_cur_hash_map;

#ifdef GLOBAL_HASHSET_LOCK
    mutable std::mutex m;
#endif

public:
    SimpleHashMap(uint32_t nBuckets, KeyExtractor key_extractor, AccessCb access_cb = {});
    ~SimpleHashMap();

    bool insert(const K& key, const V& value);
    bool upsert(const K& key, const V& value);
    bool get(const K& input_key, V& out_val);
    bool erase(const K& key, V& out_val);
    bool try_erase(const K& key);
    bool update(const K& key, auto&& update_cb);
    bool upsert_or_delete(const K& key, auto&& update_or_delete_cb);

    static void set_current_instance(SimpleHashMap< K, V, KeyExtractor, AccessCb >* hmap) { s_cur_hash_map = hmap; }
    static SimpleHashMap< K, V, KeyExtractor, AccessCb >* get_current_instance() { return s_cur_hash_map; }
    static AccessCb& get_access_cb() { return get_current_instance()->m_key_access_cb; }
    static KeyExtractor& extractor_cb() { return get_current_instance()->m_key_extract_cb; }

    template < typename... Args >
    static void call_access_cb(Args&&... args) {
        if constexpr (!std::is_same_v< AccessCb, NullCacheCb >) {
            (get_current_instance()->m_key_access_cb)(std::forward< Args >(args)...);
        }
    }
    static size_t compute_hash(const K& key) {
        size_t seed = s_start_seed;
        boost::hash_combine(seed, key);
        return seed;
    }

private:
    SimpleHashBucket< K, V, KeyExtractor, AccessCb >& get_bucket(const K& key) const;
    SimpleHashBucket< K, V, KeyExtractor, AccessCb >& get_bucket(size_t hash_code) const;
};

///////////////////////////////////////////// MultiEntryHashNode Definitions ///////////////////////////////////
template < typename V >
struct SingleEntryHashNode : public ValueEntryBase, public boost::intrusive::slist_base_hook<> {
    V m_value;
    SingleEntryHashNode(const V& value) : m_value{value} {}
};

///////////////////////////////////////////// ValueEntryRange Definitions ///////////////////////////////////

template < typename K, typename V, typename KeyExtractor, typename AccessCb >
thread_local sisl::SimpleHashMap< K, V, KeyExtractor, AccessCb >*
    sisl::SimpleHashMap< K, V, KeyExtractor, AccessCb >::s_cur_hash_map{nullptr};

///////////////////////////////////////////// SimpleHashBucket Definitions ///////////////////////////////////
template < typename K, typename V, typename KeyExtractor, typename AccessCb >
class SimpleHashBucket {
private:
#ifndef GLOBAL_HASHSET_LOCK
    mutable std::shared_mutex m_lock;
#endif
    typedef boost::intrusive::slist< SingleEntryHashNode< V > > hash_node_list_t;
    hash_node_list_t m_list;

public:
    SimpleHashBucket() = default;

    ~SimpleHashBucket() {
        auto it{m_list.begin()};
        while (it != m_list.end()) {
            SingleEntryHashNode< V >* n = &*it;
            it = m_list.erase(it);
            delete n;
        }
    }

    bool insert(const K& input_key, const V& input_value, bool overwrite_ok) {
#ifndef GLOBAL_HASHSET_LOCK
        auto holder = std::unique_lock< std::shared_mutex >(m_lock);
#endif
        SingleEntryHashNode< V >* n = nullptr;
        auto it = m_list.begin();
        for (auto itend{m_list.end()}; it != itend; ++it) {
            const K k = SimpleHashMap< K, V, KeyExtractor, AccessCb >::extractor_cb()(it->m_value);
            if (input_key > k) {
                break;
            } else if (input_key == k) {
                n = &*it;
            }
        }

        if (n == nullptr) {
            n = new SingleEntryHashNode< V >(input_value);
            m_list.insert(it, *n);
            access_cb(*n, input_key, hash_op_t::CREATE);
            return true;
        } else {
            if (overwrite_ok) {
                n->m_value = input_value;
                access_cb(*n, input_key, hash_op_t::ACCESS);
            }
            return false;
        }
    }

    bool get(const K& input_key, V& out_val) {
#ifndef GLOBAL_HASHSET_LOCK
        auto holder = std::shared_lock< std::shared_mutex >(m_lock);
#endif
        bool found{false};
        for (const auto& n : m_list) {
            const K k = SimpleHashMap< K, V, KeyExtractor, AccessCb >::extractor_cb()(n.m_value);
            if (input_key > k) {
                break;
            } else if (input_key == k) {
                out_val = n.m_value;
                found = true;
                access_cb(n, input_key, hash_op_t::ACCESS);
                break;
            }
        }
        return found;
    }

    bool erase(const K& input_key, V& out_val) {
#ifndef GLOBAL_HASHSET_LOCK
        auto holder = std::unique_lock< std::shared_mutex >(m_lock);
#endif
        return erase_unsafe(input_key, out_val, true /* call_access_cb */);
    }

    bool try_erase(const K& input_key) {
        V dummy_val;
#ifndef GLOBAL_HASHSET_LOCK
        if (m_lock.try_lock()) {
            bool ret = erase_unsafe(input_key, dummy_val, false /* call_access_cb */);
            m_lock.unlock();
            return ret;
        } else {
            return false;
        }
#endif
        return erase_unsafe(input_key, dummy_val, false);
    }

    bool upsert_or_delete(const K& input_key, auto&& update_or_delete_cb) {
#ifndef GLOBAL_HASHSET_LOCK
        auto holder = std::unique_lock< std::shared_mutex >(m_lock);
#endif
        SingleEntryHashNode< V >* n = nullptr;

        auto it = m_list.begin();
        for (auto itend{m_list.end()}; it != itend; ++it) {
            const K k = SimpleHashMap< K, V, KeyExtractor, AccessCb >::extractor_cb()(it->m_value);
            if (input_key > k) {
                break;
            } else if (input_key == k) {
                n = &*it;
                break;
            }
        }

        bool found{true};
        if (n == nullptr) {
            n = new SingleEntryHashNode< V >(V{});
            m_list.insert(it, *n);
            access_cb(*n, input_key, hash_op_t::CREATE);
            found = false;
        }

        if (update_or_delete_cb(n->m_value, found)) {
            access_cb(*n, input_key, hash_op_t::DELETE);
            m_list.erase(it);
            delete n;
        } else {
            access_cb(*n, input_key, hash_op_t::ACCESS);
        }

        return !found;
    }

    bool update(const K& input_key, auto&& update_cb) {
#ifndef GLOBAL_HASHSET_LOCK
        auto holder = std::shared_lock< std::shared_mutex >(m_lock);
#endif
        bool found{false};
        for (auto& n : m_list) {
            const K k = SimpleHashMap< K, V, KeyExtractor, AccessCb >::extractor_cb()(n.m_value);
            if (input_key > k) {
                break;
            } else if (input_key == k) {
                found = true;
                access_cb(n, input_key, hash_op_t::ACCESS);
                update_cb(n.m_value);
                break;
            }
        }
        return found;
    }

private:
    static void access_cb(const SingleEntryHashNode< V >& node, const K& key, hash_op_t op) {
        SimpleHashMap< K, V, KeyExtractor, AccessCb >::call_access_cb((const ValueEntryBase&)node, key, op);
    }

    bool erase_unsafe(const K& input_key, V& out_val, bool call_access_cb) {
        SingleEntryHashNode< V >* n = nullptr;

        auto it = m_list.begin();
        for (auto itend{m_list.end()}; it != itend; ++it) {
            const K k = SimpleHashMap< K, V, KeyExtractor, AccessCb >::extractor_cb()(it->m_value);
            if (input_key > k) {
                break;
            } else if (input_key == k) {
                n = &*it;
                break;
            }
        }

        if (n) {
            if (call_access_cb) { access_cb(*n, input_key, hash_op_t::DELETE); }
            out_val = n->m_value;
            m_list.erase(it);
            delete n;
            return true;
        }
        return false;
    }
};

///////////////////////////////////////////// SimpleHashMap Definitions ///////////////////////////////////
template < typename K, typename V, typename KeyExtractor, typename AccessCb >
SimpleHashMap< K, V, KeyExtractor, AccessCb >::SimpleHashMap(uint32_t nBuckets, KeyExtractor key_extractor,
                                                             AccessCb access_cb) :
        m_nbuckets{nBuckets}, m_key_extract_cb{std::move(key_extractor)}, m_key_access_cb{std::move(access_cb)} {
    m_buckets = new SimpleHashBucket< K, V, KeyExtractor, AccessCb >[nBuckets];
}

template < typename K, typename V, typename KeyExtractor, typename AccessCb >
SimpleHashMap< K, V, KeyExtractor, AccessCb >::~SimpleHashMap() {
    delete[] m_buckets;
}

template < typename K, typename V, typename KeyExtractor, typename AccessCb >
bool SimpleHashMap< K, V, KeyExtractor, AccessCb >::insert(const K& key, const V& value) {
#ifdef GLOBAL_HASHSET_LOCK
    std::lock_guard< std::mutex > lk(m);
#endif
    set_current_instance(this);
    return get_bucket(key).insert(key, value, false /* overwrite_ok */);
}

template < typename K, typename V, typename KeyExtractor, typename AccessCb >
bool SimpleHashMap< K, V, KeyExtractor, AccessCb >::upsert(const K& key, const V& value) {
#ifdef GLOBAL_HASHSET_LOCK
    std::lock_guard< std::mutex > lk(m);
#endif
    set_current_instance(this);
    return get_bucket(key).insert(key, value, true /* overwrite_ok */);
}

template < typename K, typename V, typename KeyExtractor, typename AccessCb >
bool SimpleHashMap< K, V, KeyExtractor, AccessCb >::get(const K& key, V& out_val) {
#ifdef GLOBAL_HASHSET_LOCK
    std::lock_guard< std::mutex > lk(m);
#endif
    set_current_instance(this);
    return get_bucket(key).get(key, out_val);
}

template < typename K, typename V, typename KeyExtractor, typename AccessCb >
bool SimpleHashMap< K, V, KeyExtractor, AccessCb >::erase(const K& key, V& out_val) {
#ifdef GLOBAL_HASHSET_LOCK
    std::lock_guard< std::mutex > lk(m);
#endif
    set_current_instance(this);
    return get_bucket(key).erase(key, out_val);
}

template < typename K, typename V, typename KeyExtractor, typename AccessCb >
bool SimpleHashMap< K, V, KeyExtractor, AccessCb >::try_erase(const K& key) {
    set_current_instance(this);
    return get_bucket(key).try_erase(key);
}

/// This is a special atomic operation where user can insert_or_update_or_erase based on condition atomically. It
/// performs differently based on certain conditions.
///
/// NOTE: This method works only if the Value is default constructible
///
/// * If the key does not exist, it will insert a default value and does the callback
///
/// * Callback should so one of the following 2 operation
///    a) The current value can be updated and return false from callback - it works like an upsert operation
///    b) Return true from callback - in that case it will behave like erase operation of the KV
///
/// Returns true if the value was inserted
template < typename K, typename V, typename KeyExtractor, typename AccessCb >
bool SimpleHashMap< K, V, KeyExtractor, AccessCb >::upsert_or_delete(const K& key, auto&& update_or_delete_cb) {
#ifdef GLOBAL_HASHSET_LOCK
    std::lock_guard< std::mutex > lk(m);
#endif
    set_current_instance(this);
    return get_bucket(key).upsert_or_delete(key, std::move(update_or_delete_cb));
}

template < typename K, typename V, typename KeyExtractor, typename AccessCb >
bool SimpleHashMap< K, V, KeyExtractor, AccessCb >::update(const K& key, auto&& update_cb) {
#ifdef GLOBAL_HASHSET_LOCK
    std::lock_guard< std::mutex > lk(m);
#endif
    set_current_instance(this);
    return get_bucket(key).update(key, std::move(update_cb));
}

template < typename K, typename V, typename KeyExtractor, typename AccessCb >
SimpleHashBucket< K, V, KeyExtractor, AccessCb >&
SimpleHashMap< K, V, KeyExtractor, AccessCb >::get_bucket(const K& key) const {
    return (m_buckets[compute_hash(key) % m_nbuckets]);
}

template < typename K, typename V, typename KeyExtractor, typename AccessCb >
SimpleHashBucket< K, V, KeyExtractor, AccessCb >&
SimpleHashMap< K, V, KeyExtractor, AccessCb >::get_bucket(size_t hash_code) const {
    return (m_buckets[hash_code % m_nbuckets]);
}

} // namespace sisl
