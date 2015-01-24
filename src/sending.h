#ifndef SENDING_H
#define SENDING_H

#include <string>
#include <vector>
#include <map>
#include <list>

#include <node.h>

#include "proton/message.h"

class Messenger;

struct InFlightMessage {
  InFlightMessage(Handle<Object> msg_, pn_message_t *pnmsg_, Handle<Function> cb_) :
    pnmsg(pnmsg_),
    retried(false),
    error(0) {
      callback = Persistent<Function>::New(cb_);
      message = Persistent<Object>::New(msg_);
    };
    
  ~InFlightMessage() {
    if(pnmsg) {
      pn_message_free(pnmsg);
    }
    message.Dispose();
    callback.Dispose();
  }

  Persistent<Object> message; 
  Persistent<Function> callback;
  pn_message_t *pnmsg;
  pn_tracker_t tracker;
  bool retried;
  int error;
};

struct QueuedWorker {
 QueuedWorker(Messenger *m);
  ~QueuedWorker();
  
  void AppendMessage(InFlightMessage *msg);
  void AppendMessages(std::list<InFlightMessage *> *srcList, 
                      std::list<InFlightMessage *>::iterator begin,
                      std::list<InFlightMessage *>::iterator end,
                      NODE_CPROTON_MUTEX_t *srcMutex);
  
protected:
  virtual void HandleQueue(void) = 0;

  Messenger *msgr;
  std::list<InFlightMessage *> messageList;
  NODE_CPROTON_MUTEX_t(mutex);
  bool active;
  uv_timer_t activityTimer;
};

struct MessageSender : QueuedWorker {
  MessageSender(Messenger *m) :
      QueuedWorker(m) {};

  ~MessageSender() {};

protected:
  virtual void HandleQueue(void);
  static void ProcessSending(uv_timer_t* handle, int status);
};

struct MessageSettler : QueuedWorker {
  MessageSettler(Messenger *m) :
      QueuedWorker(m) {};
  
  ~MessageSettler() {};
  
protected:
  virtual void HandleQueue(void);  
  static void ProcessSettling(uv_timer_t* handle, int status);
};

#endif /* SENDING_H */