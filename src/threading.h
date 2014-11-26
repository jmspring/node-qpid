// Adapted almost entirely from node_sqlite3
// Copyright (c) 2011, Konstantin Käfer <kkaefer@gmail.com>
// All rights reserved.

// https://github.com/developmentseed/node-sqlite3/blob/master/LICENSE

#ifndef NODE_CPROTON_SRC_THREADING_H
#define NODE_CPROTON_SRC_THREADING_H


#ifdef _WIN32

#include <windows.h>

    #define NODE_CPROTON_MUTEX_t(m) HANDLE m;

    #define NODE_CPROTON_MUTEX_INIT(m) m = CreateMutex(NULL, FALSE, NULL);

    #define NODE_CPROTON_MUTEX_LOCK(m) WaitForSingleObject(m, INFINITE);

    #define NODE_CPROTON_MUTEX_UNLOCK(m) ReleaseMutex(m);

    #define NODE_CPROTON_MUTEX_DESTROY(m) CloseHandle(m);

#else

#include <pthread.h>

    #define NODE_CPROTON_MUTEX_t(m) pthread_mutex_t m;

    #define NODE_CPROTON_MUTEX_INIT(m) pthread_mutex_init(&m,NULL);

    #define NODE_CPROTON_MUTEX_LOCK(m) pthread_mutex_lock(m);

    #define NODE_CPROTON_MUTEX_UNLOCK(m) pthread_mutex_unlock(m);

    #define NODE_CPROTON_MUTEX_DESTROY(m) pthread_mutex_destroy(&m);

#endif


#endif // NODE_CPROTON_SRC_THREADING_H
