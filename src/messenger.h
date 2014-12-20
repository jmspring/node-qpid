#ifndef MESSENGER_H
#define MESSENGER_H

#include <string>
#include <vector>
#include <map>
#include <list>

#include <node.h>

#include "proton/message.h"
#include "proton/messenger.h"

#include "macros.h"
#include "threading.h"
#include "protondata.h"
//#include "sending.h"

using namespace v8;
using namespace node;

#define MSG_ERROR_NONE      0
#define MSG_ERROR_INTERNAL  -1
#define MSG_ERROR_REJECTED  -2
#define MSG_ERROR_STATUS_UNKNOWN -3
#define MSG_ERROR_ABORTED   -4

struct MessageInfo {
  pn_message_t *message;
  std::string  subscription_address;
};

typedef std::vector<MessageInfo *> Messages;

class Messenger : public node::ObjectWrap {
 public:
  static Persistent<FunctionTemplate> constructor_template;
  static void Init(Handle<Object> target);

  struct Baton {

    uv_work_t request;
    Messenger* msgr;
    Persistent<Function> callback;

    int error_code;
    std::string error_message;

    Baton(Messenger* msgr_, Handle<Function> cb_) :
        msgr(msgr_) {
      msgr->Ref();
      request.data = this;
      callback = Persistent<Function>::New(cb_);
      error_code = 0;
    }   

    virtual ~Baton() {
      msgr->Unref();
      callback.Dispose();
    }

  };

  struct SubscribeBaton : Baton {
    int subscriptionIndex;
    pn_data_t *filter_key;
    pn_data_t *filter_value;

    SubscribeBaton(Messenger* msgr_, int subIndex, pn_data_t *key, pn_data_t *value, Handle<Function> cb_) :
      Baton(msgr_, cb_),
      subscriptionIndex(subIndex),
      filter_key(key),
      filter_value(value) {};
      
    ~SubscribeBaton() {
      if(filter_key) {
        pn_data_free(filter_key);
      }
      if(filter_value) {
        pn_data_free(filter_value);
      }
      filter_value = filter_key = NULL;
    }
  };
  
  struct AddSourceFilterBaton : Baton {

    std::string address;
    pn_data_t *filter_key;
    pn_data_t *filter_value;

    AddSourceFilterBaton(Messenger* msgr_, Handle<Function> cb_, const char* address_, pn_data_t *filter_key_, pn_data_t *filter_value_) :
      Baton(msgr_, cb_), address(address_), filter_key(filter_key_), filter_value(filter_value_) {}

  };

  struct SendBaton : Baton {
  
    SendBaton(Messenger* msgr_, Handle<Function> cb_) :
      Baton(msgr_, cb_) {}

  };

  struct SendSettlingBaton : Baton {
  
    SendSettlingBaton(Messenger* msgr_, Handle<Function> cb_) :
      Baton(msgr_, cb_) {}

  };

  struct Async;

  struct ReceiveBaton : Baton {

    Async* async;

    ReceiveBaton(Messenger* msgr_, Handle<Function> cb_) : 
      Baton(msgr_, cb_) {} 

  };

  struct Async {
    uv_async_t watcher;
    Messenger* msgr;
    Messages data;
    NODE_CPROTON_MUTEX_t(mutex);
    bool completed;
    int retrieved;

    // Store the emitter here because we don't have
    // access to the baton in the async callback.
    Persistent<Function> emitter;

    Async(Messenger* m, uv_async_cb async_cb) :
            msgr(m), completed(false), retrieved(0) {
        watcher.data = this;
        NODE_CPROTON_MUTEX_INIT(mutex);
        msgr->Ref();
        uv_async_init(uv_default_loop(), &watcher, async_cb);
    }

    ~Async() {
        msgr->Unref();
        emitter.Dispose();
        NODE_CPROTON_MUTEX_DESTROY(mutex)
    }
  };
  
  struct Subscription {
    Subscription(std::string address_, Handle<Function> cb_) :
        address(address_),
        callback(Persistent<Function>::New(cb_)) {};
        
    std::string address;
    Persistent<Function> callback;
  };
  
  Subscription *GetSubscriptionByAddress(std::string addr);
  Subscription *GetSubscriptionByIndex(unsigned long idx);
  Subscription *GetSubscriptionByHandle(pn_subscription_t *sub);
  unsigned long AddSubscription(Subscription *sub);
  bool SetSubscriptionHandle(unsigned long idx, pn_subscription_t *sub);
  
  void SetSourceFilter(std::string & address, pn_data_t *key, pn_data_t *value);

  // protected access methods
  int MessengerGetOutgoingWindow(void);
  int MessengerGetOutgoing(void);
  bool MessengerGetBuffered(pn_tracker_t tracker);
  pn_status_t MessengerGetStatus(pn_tracker_t tracker);
  int MessengerSettleOutgoing(pn_tracker_t tracker);
  
  // error stuff
  static int MapPNStatusToError(pn_status_t status);
  static std::string MapErrorToString(int error);
  
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

  virtual ~MessageSender() {};

protected:
  virtual void HandleQueue(void);
  static void ProcessSending(uv_timer_t* handle, int status);
};

struct MessageSettler : QueuedWorker {
  MessageSettler(Messenger *m) :
      QueuedWorker(m) {};
  
  virtual ~MessageSettler() {};
  
protected:
  virtual void HandleQueue(void);  
  static void ProcessSettling(uv_timer_t* handle, int status);
};

  
  
  
  
  

 private:
  Messenger();
  ~Messenger();

  WORK_DEFINITION(Send)
  WORK_DEFINITION(Subscribe)
  WORK_DEFINITION(AddSourceFilter)
  WORK_DEFINITION(Stop)
  WORK_DEFINITION(Put)
  WORK_DEFINITION(Receive)
  WORK_DEFINITION(SendSettling)

  static void AsyncReceive(uv_async_t* handle, int status);
  static void CloseEmitter(uv_handle_t* handle);
  static Local<Object> MessageToJS(pn_message_t* message);
  static pn_message_t * JSToMessage(Local<Object>);

  static Handle<Value> New(const Arguments& args);
  std::string address;
  pn_messenger_t * messenger;
  pn_messenger_t * receiver;
  bool receiving;
  bool receiveWait;
  NODE_CPROTON_MUTEX_t mutex;
  ReceiveBaton * receiveWaitBaton;
  
  MessageSender *messageSender;
  MessageSettler *messageSettler;
  
  std::vector<Subscription *> _subscriptions;
  std::map<std::string, unsigned long> _addressToSubscriptionMap;
  std::map<pn_subscription_t *, unsigned long> _handleToSubscriptionMap;
  
  int subscriptions;
};

#endif
