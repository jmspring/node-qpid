#ifndef MESSENGER_H
#define MESSENGER_H

#include <string>
#include <vector>

#include <node.h>

#include "proton/message.h"
#include "proton/messenger.h"

#include "macros.h"
#include "threading.h"

using namespace v8;
using namespace node;

typedef std::vector<pn_message_t*> Messages;

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

    std::string address;

    SubscribeBaton(Messenger* msgr_, Handle<Function> cb_, const char* address_) :
      Baton(msgr_, cb_), address(address_) {}

  };
  
  struct AddSourceFilterBaton : Baton {

    std::string address;
    pn_data_t *filter_key;
    pn_data_t *filter_value;

    AddSourceFilterBaton(Messenger* msgr_, Handle<Function> cb_, const char* address_, pn_data_t *filter_key_, pn_data_t *filter_value_) :
      Baton(msgr_, cb_), address(address_), filter_key(filter_key_), filter_value(filter_value_) {}

  };
/*
  struct RemoveSourceFilterBaton : Baton {

    std::string address;
    std::string filter_key;

    AddSourceFilterBaton(Messenger* msgr_, Handle<Function> cb_, const char* address_, const char *filter_key_, pn_type_t filter_type_, void *filter_value_) :
      Baton(msgr_, cb_), address(address_), filter_key(filter_key_) {}

  };
*/
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
    NODE_CPROTON_MUTEX_t;
    bool completed;
    int retrieved;

    // Store the emitter here because we don't have
    // access to the baton in the async callback.
    Persistent<Function> emitter;

    Async(Messenger* m, uv_async_cb async_cb) :
            msgr(m), completed(false), retrieved(0) {
        watcher.data = this;
        NODE_CPROTON_MUTEX_INIT
        msgr->Ref();
        uv_async_init(uv_default_loop(), &watcher, async_cb);
    }

    ~Async() {
        msgr->Unref();
        emitter.Dispose();
        NODE_CPROTON_MUTEX_DESTROY
    }
  };


 private:
  Messenger();
  ~Messenger();

  WORK_DEFINITION(Send)
  WORK_DEFINITION(Subscribe)
  WORK_DEFINITION(AddSourceFilter)
  //WORK_DEFINITION(RemoveSourceFilter)
  WORK_DEFINITION(Stop)
  WORK_DEFINITION(Put)
  WORK_DEFINITION(Receive)

  static void AsyncReceive(uv_async_t* handle, int status);
  static void CloseEmitter(uv_handle_t* handle);
  static Local<Object> MessageToJS(pn_message_t* message);
  static pn_message_t * JSToMessage(Local<Object>);
  
  // pn_data_parsing routines
  static pn_type_t JSTypeToPNType(std::string type);
  static std::string PNTypeToJSType(pn_type_t type);
  static bool IsSimpleValue(pn_type_t type);
  static Handle<Value> GetSimpleValue(pn_data_t *data);
  static Handle<Value> GetDescribedValue(pn_data_t *data);
  static Handle<Value> GetArrayValue(pn_data_t *data);
  static Handle<Value> GetListValue(pn_data_t *data);
  static Handle<Value> GetMapValue(pn_data_t *data);
  static Handle<Value> ParseValue(pn_data_t *data);
  static Local<Array> ParsePnData(pn_data_t *data);  
  static pn_data_t *ParseJSData(Handle<Value> jsval);
  static pn_data_t *GetSimpleJSValue(pn_type_t type, Local<Value> jsval);
  static pn_data_t *GetDescribedJSValue(Handle<Array> array);
  static pn_data_t *GetArrayOrListJSValue(pn_type_t type, Handle<Array> array);
  static pn_data_t *GetArrayJSValue(Handle<Array> array);
  static pn_data_t *GetListJSValue(Handle<Array> array);
  static pn_data_t *GetMapJSValue(Handle<Array> array);

  static Handle<Value> New(const Arguments& args);
  std::string address;
  pn_messenger_t * messenger;
  pn_messenger_t * receiver;
  bool receiving;
  bool receiveWait;
  ReceiveBaton * receiveWaitBaton;
  int subscriptions;
  
 NODE_CPROTON_MUTEX_t

};

#endif
