// Adapted almost entirely from node_sqlite3
// Copyright (c) 2011, Konstantin Käfer <kkaefer@gmail.com>
// All rights reserved.

// https://github.com/developmentseed/node-sqlite3/blob/master/LICENSE

#ifndef NODE_CPROTON_SRC_ASYNC_H
#define NODE_CPROTON_SRC_ASYNC_H

#include "threading.h"
#include <node_version.h>

// Generic uv_async handler.
template <class Item, class Parent> class Async {
    typedef void (*Callback)(Parent* parent, Item* item);

protected:
    uv_async_t watcher;
    NODE_CPROTON_MUTEX_t(mutex);
    std::vector<Item*> data;
    Callback callback;
public:
    Parent* parent;

public:
    Async(Parent* parent_, Callback cb_)
        : callback(cb_), parent(parent_) {
        watcher.data = this;
        NODE_CPROTON_MUTEX_INIT(mutex)
        uv_async_init(uv_default_loop(), &watcher, listener);
    }

    static void listener(uv_async_t* handle, int status) {
        Async* async = static_cast<Async*>(handle->data);
        std::vector<Item*> rows;
        NODE_CPROTON_MUTEX_LOCK(&async->mutex)
        rows.swap(async->data);
        NODE_CPROTON_MUTEX_UNLOCK(&async->mutex)
        for (unsigned int i = 0, size = rows.size(); i < size; i++) {
#if NODE_VERSION_AT_LEAST(0, 7, 9)
            uv_unref((uv_handle_t *)&async->watcher);
#else
            uv_unref(uv_default_loop());
#endif
            async->callback(async->parent, rows[i]);
        }
    }

    static void close(uv_handle_t* handle) {
        assert(handle != NULL);
        assert(handle->data != NULL);
        Async* async = static_cast<Async*>(handle->data);
        delete async;
        handle->data = NULL;
    }

    void finish() {
        // Need to call the listener again to ensure all items have been
        // processed. Is this a bug in uv_async? Feels like uv_close
        // should handle that.
        listener(&watcher, 0);
        uv_close((uv_handle_t*)&watcher, close);
    }

    void add(Item* item) {
        // Make sure node runs long enough to deliver the messages.
#if NODE_VERSION_AT_LEAST(0, 7, 9)
        uv_ref((uv_handle_t *)&watcher);
#else
        uv_ref(uv_default_loop());
#endif
        NODE_CPROTON_MUTEX_LOCK(&mutex);
        data.push_back(item);
        NODE_CPROTON_MUTEX_UNLOCK(&mutex)
    }

    void send() {
        uv_async_send(&watcher);
    }

    void send(Item* item) {
        add(item);
        send();
    }

    ~Async() {
        NODE_CPROTON_MUTEX_DESTROY(mutex)
    }
};

#endif
