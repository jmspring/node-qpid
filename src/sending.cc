#include <node.h>
#include <string>
#include <list>

#include "messenger.h"
#include "async.h"

using namespace v8;
using namespace node;
using namespace std;

Messenger::QueuedWorker::QueuedWorker(Messenger *m) :
      msgr(m),
      active(false) {
  NODE_CPROTON_MUTEX_INIT(mutex);
};

Messenger::QueuedWorker::~QueuedWorker() {
  std::list<InFlightMessage *>::iterator it = messageList.begin();
  NODE_CPROTON_MUTEX_LOCK(&mutex);
  while(it != messageList.end()) {
    InFlightMessage *msg = *it;
    if(msg) {
      delete(msg);
      *it = NULL;
    }
    it = messageList.erase(it);
  }
  NODE_CPROTON_MUTEX_DESTROY(mutex);
};


void Messenger::QueuedWorker::AppendMessage(InFlightMessage *msg) {
  bool enableWorker;
  NODE_CPROTON_MUTEX_LOCK(&mutex);
  messageList.push_back(msg);
  enableWorker = !active;
  NODE_CPROTON_MUTEX_UNLOCK(&mutex);
  
  if(enableWorker) {
    HandleQueue();
  }
}

void Messenger::QueuedWorker::AppendMessages(std::list<InFlightMessage *> *srcList, 
                                  std::list<InFlightMessage *>::iterator begin,
                                  std::list<InFlightMessage *>::iterator end,
                                  NODE_CPROTON_MUTEX_t *srcMutex) {
  bool enableWorker;
  
  NODE_CPROTON_MUTEX_LOCK(&mutex);
  if(srcMutex) {
    NODE_CPROTON_MUTEX_LOCK(srcMutex);
  }
  messageList.splice(messageList.end(), *srcList, begin, end);
  enableWorker = !active;
  if(srcMutex) {
    NODE_CPROTON_MUTEX_UNLOCK(srcMutex);
  }
  NODE_CPROTON_MUTEX_UNLOCK(&mutex);
  
  if(enableWorker) {
    HandleQueue();
  }  
}

void Messenger::MessageSender::HandleQueue(void) {
  NODE_CPROTON_MUTEX_LOCK(&mutex);
  if(!active) {
    active = true;
    uv_timer_init(uv_default_loop(), &activityTimer);
    activityTimer.data = this;
    uv_timer_start(&activityTimer, ProcessSending, 5, 0);
  }
  NODE_CPROTON_MUTEX_UNLOCK(&mutex);
}

void Messenger::MessageSender::ProcessSending(uv_timer_t *handle, int status) {
  MessageSender *sender = static_cast<MessageSender *>(handle->data);
  pn_messenger_t* messenger = sender->msgr->messenger;
  
  // Determine how many messages can be sent in this iteration.
  NODE_CPROTON_MUTEX_LOCK(&sender->mutex)
  int outboundMessages = sender->messageList.size();
  int windowSize = sender->msgr->MessengerGetOutgoingWindow();
  int availableSlots = 0;
  if(windowSize) {
    int outgoing = sender->msgr->MessengerGetOutgoing();
    if(outgoing < windowSize) {
      availableSlots = windowSize - outgoing;
    }
  } else {
    // NOTE - if the window size is 0, then it will be impossible to track the
    // status of a particular tracker.  In that case (for settling purposes), 
    // PN_STATUS_UNKNOWN will not be an error.  For sending, we will just try 
    // and send one message.
    availableSlots = 1;
  }
  if(outboundMessages < availableSlots) {
    availableSlots = outboundMessages;
  }
  NODE_CPROTON_MUTEX_UNLOCK(&sender->mutex)

  int err = 0;
  std::list<InFlightMessage *>::iterator it = sender->messageList.begin();
  while(availableSlots) {
    InFlightMessage *msg = *it;
    if(msg) {
      err = pn_messenger_put( messenger, msg->pnmsg );
      if(err) {
        msg->error = err;
      } else {
        msg->tracker = pn_messenger_outgoing_tracker( messenger );
      }
    }
    availableSlots--;
    it++;
  }

  // messages sent?  
  if(it != sender->messageList.begin()) {
    err = pn_messenger_send(messenger, -1);
    if(err) {
      // TODO -- bubble this up
    }
  } else {
    err = pn_messenger_work(messenger, 0);
  }
    
  if(it != sender->messageList.begin()) {
    sender->msgr->messageSettler->AppendMessages(&sender->messageList, 
                                                 sender->messageList.begin(),
                                                 it,
                                                 &sender->mutex);
  }
  
  NODE_CPROTON_MUTEX_LOCK(&sender->mutex)
  bool keepSending = ((sender->msgr->MessengerGetOutgoing() > 0) || (sender->messageList.size() > 0));
  sender->active = false;
  NODE_CPROTON_MUTEX_UNLOCK(&sender->mutex)
  if(keepSending) {
    sender->HandleQueue();
  }
}

void Messenger::MessageSettler::HandleQueue(void) {
  NODE_CPROTON_MUTEX_LOCK(&mutex);
  if(!active) {
    active = true;
    uv_timer_init(uv_default_loop(), &activityTimer);
    activityTimer.data = this;
    uv_timer_start(&activityTimer, ProcessSettling, 5, 0);
  }
  NODE_CPROTON_MUTEX_UNLOCK(&mutex);
}

void Messenger::MessageSettler::ProcessSettling(uv_timer_t *handle, int status) {
  HandleScope scope;

  MessageSettler *settler = static_cast<MessageSettler *>(handle->data);
  pn_messenger_t* messenger = settler->msgr->messenger;
  
  std::list<InFlightMessage *>::iterator it;
  std::list<InFlightMessage *> abortedMessages;
  
  NODE_CPROTON_MUTEX_LOCK(&settler->mutex)
  int needingSettling = settler->messageList.size();
  NODE_CPROTON_MUTEX_UNLOCK(&settler->mutex)
  
  it = settler->messageList.begin();
  while(needingSettling) {
    bool processed = false;
    
    InFlightMessage *msg = *it;
    if(msg) {
      if(msg->error) {
        // an error in sending
        processed = true;
      } else {
        // once we hit a message that isn't buffered
        bool buffered = settler->msgr->MessengerGetBuffered(msg->tracker);
        pn_status_t status = settler->msgr->MessengerGetStatus(msg->tracker);
        if(!buffered) {
          // need to handle errors!!!
          if((status == PN_STATUS_PENDING) || (status = PN_STATUS_SETTLED)) {
            // consider message ok
          } else {
            if(status != PN_STATUS_SETTLED) {
              msg->error = Messenger::MapPNStatusToError(status);
            }
          }
          settler->msgr->MessengerSettleOutgoing(msg->tracker);
          processed = true;
        } else {
          if(status != PN_STATUS_PENDING) {
            settler->msgr->MessengerSettleOutgoing(msg->tracker);
            if((status == PN_STATUS_ABORTED) && !msg->retried) {
              msg->retried = true;
              abortedMessages.push_back(msg);
              *it = msg = NULL;
            } else {
              msg->error = Messenger::MapPNStatusToError(status);
            }
            processed = true;
          }
          // exit out of the loop since we reached a message still buffered
          needingSettling = 0; 
        }
      }
          
      if(msg && processed) {
        if(!msg->callback.IsEmpty() && msg->callback->IsFunction()) {
          if(msg->error) {
            std::string errorString = Messenger::MapErrorToString(msg->error);
            Local<Value> args[] = { String::New(errorString.c_str()), Local<Object>::New(msg->message) }; 
            msg->callback->Call(Context::GetCurrent()->Global(), 2, args);
          } else {
            Local<Value> args[] = { Local<Value>::New(Null()), Local<Object>::New(msg->message) }; 
            msg->callback->Call(Context::GetCurrent()->Global(), 2, args);
          }
        }
        delete(msg);
      }
    }
    
    if(processed) {
      if(needingSettling) {
        needingSettling--;
      }
      it = settler->messageList.erase(it);
    }
  }
  
  if(abortedMessages.size() > 0) {
    settler->msgr->messageSender->AppendMessages(&abortedMessages, 
                                                abortedMessages.begin(),
                                                abortedMessages.end(),
                                                NULL);
  }
  
  NODE_CPROTON_MUTEX_LOCK(&settler->mutex)
  //settler->messageList.erase(settler->messageList.begin(), it);
  bool keepSending = (settler->messageList.size() > 0);
  settler->active = false;
  NODE_CPROTON_MUTEX_UNLOCK(&settler->mutex)
  
  if(keepSending) {
    settler->HandleQueue();
  }
}