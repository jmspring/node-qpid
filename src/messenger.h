#ifndef MESSENGER_H
#define MESSENGER_H

#include <string>
#include <vector>
#include <map>

#include <node.h>

#include "proton/message.h"
#include "proton/messenger.h"

#include "macros.h"
#include "threading.h"
#include "protondata.h"

using namespace v8;
using namespace node;

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

    pn_message_t * msg;
    pn_tracker_t tracker;

    SendBaton(Messenger* msgr_, Handle<Function> cb_, pn_message_t * msg_) :
      Baton(msgr_, cb_), msg(msg_) {}

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
        NODE_CPROTON_MUTEX_INIT(mutex)
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

 private:
  Messenger();
  ~Messenger();

  WORK_DEFINITION(Send)
  WORK_DEFINITION(Subscribe)
  WORK_DEFINITION(AddSourceFilter)
  WORK_DEFINITION(Stop)
  WORK_DEFINITION(Put)
  WORK_DEFINITION(Receive)

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
  NODE_CPROTON_MUTEX_t(mutex);
  ReceiveBaton * receiveWaitBaton;
  
  std::vector<Subscription *> _subscriptions;
  std::map<std::string, unsigned long> _addressToSubscriptionMap;
  std::map<pn_subscription_t *, unsigned long> _handleToSubscriptionMap;
  
  int subscriptions;
};

#endif
