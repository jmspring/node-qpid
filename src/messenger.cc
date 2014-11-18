#define BUILDING_NODE_EXTENSION
#include <node.h>
#include <iostream>
#include <vector>
#include <string>
#include <algorithm>
#include <sstream>

#include <string.h>

#include "messenger.h"
#include "async.h"


using namespace v8;
using namespace node;
using namespace std;

Messenger::Messenger() { };

// Messenger::~Messenger() { };

Persistent<FunctionTemplate> Messenger::constructor_template;

void Messenger::Init(Handle<Object> target) {
  HandleScope scope;

  Local<FunctionTemplate> t = FunctionTemplate::New(New);

  constructor_template = Persistent<FunctionTemplate>::New(t);
  constructor_template->InstanceTemplate()->SetInternalFieldCount(1);
  constructor_template->SetClassName(String::NewSymbol("Messenger"));

  NODE_SET_PROTOTYPE_METHOD(constructor_template, "send", Send);
  NODE_SET_PROTOTYPE_METHOD(constructor_template, "subscribe", Subscribe);
  NODE_SET_PROTOTYPE_METHOD(constructor_template, "receive", Receive);
  NODE_SET_PROTOTYPE_METHOD(constructor_template, "stop", Stop);
  NODE_SET_PROTOTYPE_METHOD(constructor_template, "addsourcefilter", AddSourceFilter);

  target->Set(String::NewSymbol("Messenger"),
    constructor_template->GetFunction());
 
}

Handle<Value> Messenger::New(const Arguments& args) {
  HandleScope scope;

  Messenger* msgr = new Messenger();

  pn_messenger_t* messenger = pn_messenger(NULL);
  pn_messenger_start(messenger);
  msgr->messenger = messenger;

  // Temporary fix 
  pn_messenger_set_outgoing_window(msgr->messenger, 1);

  pn_messenger_t* receiver = pn_messenger(NULL);
  pn_messenger_set_incoming_window(receiver, 1);
  pn_messenger_start(receiver);
  msgr->receiver = receiver;

  // How long to block while receiving.  Should surface this as an option
  pn_messenger_set_timeout(msgr->receiver, 50);

  msgr->receiving = false;
  msgr->receiveWait = false;
  msgr->subscriptions = 0;

  msgr->Wrap(args.This());

  return args.This();
}

Handle<Value> Messenger::Subscribe(const Arguments& args) {
  HandleScope scope;

  Messenger* msgr = ObjectWrap::Unwrap<Messenger>(args.This());

  REQUIRE_ARGUMENT_STRING(0, addr);
  OPTIONAL_ARGUMENT_FUNCTION(1, callback);

  msgr->address = *addr;

  SubscribeBaton* baton = new SubscribeBaton(msgr, callback, *addr);
  
  Work_BeginSubscribe(baton);

  return args.This();
  
}

void Messenger::Work_BeginSubscribe(Baton* baton) {
  int status = uv_queue_work(uv_default_loop(),
    &baton->request, Work_Subscribe, (uv_after_work_cb)Work_AfterSubscribe);

  assert(status == 0);

}

void Messenger::Work_Subscribe(uv_work_t* req) {

  SubscribeBaton* baton = static_cast<SubscribeBaton*>(req->data);

  pn_messenger_subscribe(baton->msgr->receiver, baton->address.c_str());

}

void Messenger::Work_AfterSubscribe(uv_work_t* req) {

  SubscribeBaton* baton = static_cast<SubscribeBaton*>(req->data);

  baton->msgr->subscriptions++;

  if (baton->error_code > 0) {
    /* Local<Value> err = Exception::Error(
    String::New(baton->error_message.c_str()));
    Local<Value> argv[2] = { Local<Value>::New(String::New("error")), err };
    MakeCallback(baton->obj, "emit", 2, argv); */
  } else {
    if (!baton->callback.IsEmpty() && baton->callback->IsFunction()) {
      
      Local<Value> args[] = { Local<Value>::New(Null()), String::New(baton->address.c_str()) }; 
      baton->callback->Call(Context::GetCurrent()->Global(), 2, args);
      
    } else {

      Local<Value> args[] = { String::New("subscribed"), String::New(baton->address.c_str()) };
      EMIT_EVENT(baton->msgr->handle_, 2, args);

    }
  }

  if (baton->msgr->receiveWait) {

    Work_BeginReceive(baton->msgr->receiveWaitBaton);

  }

  delete baton;

}

static std::string lowercase(std::string & str) 
{
  std::string rval;
  std::transform(str.begin(), str.end(), rval.begin(), ::tolower);
  return rval;
}

static const std::string emptystring("");

pn_data_t *get_data_from_type_value(std::string & type, std::string & value, std::string described = emptystring)
{
  pn_data_t *data = pn_data(0);
  istringstream istream(value);
  
  if(type.compare("NULL") == 0) {
    pn_data_put_null(data);
  } else if(type.compare("BOOL") == 0) {
    bool val = lowercase(value).compare("false");
    pn_data_put_bool(data, val);    
  } else if(type.compare("UBYTE") == 0) {
    uint8_t val;
    istream >> val;
    pn_data_put_ubyte(data, val);    
  } else if(type.compare("BYTE") == 0) {
    int8_t val;
    istream >> val;
    pn_data_put_byte(data, val);    
  } else if(type.compare("USHORT") == 0) {
    uint16_t val;
    istream >> val;
    pn_data_put_ushort(data, val);    
  } else if(type.compare("SHORT") == 0) {
    int16_t val;
    istream >> val;
    pn_data_put_short(data, val);    
  } else if(type.compare("UINT") == 0) {
    uint32_t val;
    istream >> val;
    pn_data_put_uint(data, val);    
  } else if(type.compare("INT") == 0) {
    int32_t val;
    istream >> val;
    pn_data_put_int(data, val);    
  } else if(type.compare("CHAR") == 0) {
    pn_char_t val;
    istream >> val;
    pn_data_put_char(data, val);    
  } else if(type.compare("ULONG") == 0) {
    uint64_t val;
    istream >> val;
    pn_data_put_ulong(data, val);    
  } else if(type.compare("LONG") == 0) {
    int64_t val;
    istream >> val;
    pn_data_put_long(data, val);    
  } else if(type.compare("TIMESTAMP") == 0) {
    pn_timestamp_t val;
    istream >> val;
    pn_data_put_timestamp(data, val);    
  } else if(type.compare("FLOAT") == 0) {
    float val;
    istream >> val;
    pn_data_put_float(data, val);    
  } else if(type.compare("DOUBLE") == 0) {
    double val;
    istream >> val;
    pn_data_put_double(data, val);    
  } else if(type.compare("DECIMAL32") == 0) {
    pn_decimal32_t val;
    istream >> val;
    pn_data_put_double(data, val);    
  } else if(type.compare("DECIMAL64") == 0) {
    pn_decimal64_t val;
    istream >> val;
    pn_data_put_double(data, val);    
  } else if(type.compare("DECIMAL128") == 0) {
    pn_data_free(data);
    data = NULL;   
  } else if(type.compare("UUID") == 0) {
    uuid_t uu;
    pn_uuid_t puu;
    uuid_parse(value.c_str(), uu);
    memcpy(puu.bytes, uu, 16);
    pn_data_put_uuid(data, puu);    
  } else if(type.compare("BINARY") == 0) {
    pn_data_free(data);
    data = NULL;
  } else if(type.compare("STRING") == 0) {
    pn_bytes_t val = { value.length(), value.c_str() };
    pn_data_put_string(data, val);    
  } else if(type.compare("SYMBOL") == 0) {
    pn_bytes_t val = { value.length(), value.c_str() };
    pn_data_put_symbol(data, val);    
  } else if(type.compare("DESCRIBED") == 0) {
    // currently we only support string for described data
    pn_data_put_described(data);
    pn_data_enter(data);
    pn_bytes_t desc = { described.length(), described.c_str() };
    pn_bytes_t val = { value.length(), value.c_str() };
    pn_data_put_symbol(data, desc);
    pn_data_put_string(data, val);
    pn_data_exit(data);
    pn_data_rewind(data);
  } else if(type.compare("ARRAY") == 0) {
    pn_data_free(data);
    data = NULL;
  } else if(type.compare("LIST") == 0) {
    pn_data_free(data);
    data = NULL;
  } else if(type.compare("MAP") == 0) {
    pn_data_free(data);
    data = NULL;
  }
  
  return data;  
}

Handle<Value> Messenger::AddSourceFilter(const Arguments& args) {
  HandleScope scope;

  Messenger* msgr = ObjectWrap::Unwrap<Messenger>(args.This());

  REQUIRE_ARGUMENT_STRING(0, addr);
  REQUIRE_ARGUMENT_STRING(1, key);
  REQUIRE_ARGUMENT_STRING(2, type_);
  REQUIRE_ARGUMENT_STRING(3, value_);
  OPTIONAL_ARGUMENT_FUNCTION(4, callback);
  
  std::string typestr = std::string(*type_);
  std::string valuestr = std::string(*value_);
  std::string describestr = std::string(*key);
  // TODO -- hack, assume described type "description" is equal to the key
  pn_data_t *data = get_data_from_type_value(typestr, valuestr, describestr);

  if(data) {
    msgr->address = *addr;
  
    AddSourceFilterBaton *baton = new AddSourceFilterBaton(msgr, callback, *addr, *key, pn_data_type(data), data);
  
    Work_BeginAddSourceFilter(baton);
  }

  return args.This();
  
}

void Messenger::Work_BeginAddSourceFilter(Baton* baton) {
  int status = uv_queue_work(uv_default_loop(),
    &baton->request, Work_AddSourceFilter, (uv_after_work_cb)Work_AfterAddSourceFilter);

  assert(status == 0);

}

void Messenger::Work_AddSourceFilter(uv_work_t* req) {

  AddSourceFilterBaton* baton = static_cast<AddSourceFilterBaton*>(req->data);
  
  // TODO - add check for subscribed to address
  pn_link_t *link = pn_messenger_get_link(baton->msgr->receiver, baton->address.c_str(), 0);
  if(link) {
    pn_terminus_t *sr = pn_link_source(link);
    if(sr) {
      pn_data_t *filter = pn_terminus_filter(sr);
      if(filter) {
        if(pn_data_size(filter) == 0) {
          pn_data_put_map(filter);
        }
        pn_data_next(filter);
        pn_data_enter(filter);
        while(pn_data_next(filter)) {
          // go to the end of the map
          ;
        }
        pn_bytes_t key = { baton->filter_key.length(), baton->filter_key.c_str() };
        pn_data_put_symbol(filter, key);
        pn_data_append(filter, baton->filter_value);
        pn_data_exit(filter);
        pn_data_rewind(filter);
      }
    }
  }
}

void Messenger::Work_AfterAddSourceFilter(uv_work_t* req) {

  AddSourceFilterBaton* baton = static_cast<AddSourceFilterBaton*>(req->data);
  
  if(baton->filter_value) {
    pn_data_free(baton->filter_value);
    baton->filter_value = NULL;
  }

  baton->msgr->subscriptions++;

  if (!baton->callback.IsEmpty() && baton->callback->IsFunction()) {
    
    Local<Value> args[] = { Local<Value>::New(Null()), String::New(baton->address.c_str()) }; 
    baton->callback->Call(Context::GetCurrent()->Global(), 2, args);
    
  } else {

    Local<Value> args[] = { String::New("addedsourcefilter"), String::New(baton->address.c_str()) };
    EMIT_EVENT(baton->msgr->handle_, 2, args);

  }

  delete baton;
}

Handle<Value> Messenger::Send(const Arguments& args) {
  HandleScope scope;
  
  Messenger* msgr = ObjectWrap::Unwrap<Messenger>(args.This());

  REQUIRE_ARGUMENT_OBJECT(0, obj);
  OPTIONAL_ARGUMENT_FUNCTION(1, callback);

  pn_message_t* msg = JSToMessage(obj);

  SendBaton* baton = new SendBaton(msgr, callback, msg);
  
  Work_BeginSend(baton);

  return Undefined();
    
}

void Messenger::Work_BeginSend(Baton* baton) {
  int status = uv_queue_work(uv_default_loop(),
    &baton->request, Work_Send, (uv_after_work_cb)Work_AfterSend);

  assert(status == 0);

}

void Messenger::Work_Send(uv_work_t* req) {

  SendBaton* baton = static_cast<SendBaton*>(req->data);
  pn_messenger_t* messenger = baton->msgr->messenger;
  pn_message_t* message = baton->msg;

  assert(!pn_messenger_put(messenger, message));
  baton->tracker = pn_messenger_outgoing_tracker(messenger);

  assert(!pn_messenger_send(messenger, -1));

  pn_message_free(message);

}

void Messenger::Work_AfterSend(uv_work_t* req) {
  HandleScope scope;
  SendBaton* baton = static_cast<SendBaton*>(req->data);

  if (baton->error_code > 0) {
    Local<Value> err = Exception::Error(String::New(baton->error_message.c_str()));
    Local<Value> argv[] = { err };
    baton->callback->Call(Context::GetCurrent()->Global(), 1, argv);
  } else {
    Local<Value> argv[1];
    baton->callback->Call(Context::GetCurrent()->Global(), 0, argv);
  }

  delete baton;

}

Handle<Value> Messenger::Receive(const Arguments& args) {
  HandleScope scope;

  Messenger* msgr = ObjectWrap::Unwrap<Messenger>(args.This());

  if (!msgr->receiving) {

    Local<Function> emitter = Local<Function>::Cast((msgr->handle_)->Get(String::NewSymbol("emit")));
    ReceiveBaton* baton = new ReceiveBaton(msgr, emitter);

    if (msgr->subscriptions > 0) {

      Work_BeginReceive(baton);

    } else {

      msgr->receiveWait = true;
      msgr->receiveWaitBaton = baton;

    }

  }

  return Undefined();

}

void Messenger::Work_BeginReceive(Baton *baton) {

  ReceiveBaton* receive_baton = static_cast<ReceiveBaton*>(baton);

  receive_baton->async = new Async(receive_baton->msgr, AsyncReceive);
  receive_baton->async->emitter = Persistent<Function>::New(receive_baton->callback);

  receive_baton->msgr->receiving = true;

  receive_baton->msgr->receiveWait = false;

  int status = uv_queue_work(uv_default_loop(),
    &baton->request, Work_Receive, (uv_after_work_cb)Work_AfterReceive);

  assert(status == 0);

} 

void Messenger::CloseEmitter(uv_handle_t* handle) {

  assert(handle != NULL);
  assert(handle->data != NULL);
  Async* async = static_cast<Async*>(handle->data);
  delete async;
  handle->data = NULL;

}

void Messenger::Work_Receive(uv_work_t* req) {

  ReceiveBaton* baton = static_cast<ReceiveBaton*>(req->data);
  pn_messenger_t* receiver = baton->msgr->receiver;
  Async* async = baton->async;

  while (baton->msgr->receiving) {

    pn_messenger_recv(receiver, 1024);

    while(pn_messenger_incoming(receiver)) {

      pn_message_t* message = pn_message();
      pn_messenger_get(receiver, message);

      NODE_CPROTON_MUTEX_LOCK(&async->mutex)
      async->data.push_back(message);
      NODE_CPROTON_MUTEX_UNLOCK(&async->mutex)

      uv_async_send(&async->watcher);
      
      pn_tracker_t tracker = pn_messenger_incoming_tracker(receiver);
      pn_messenger_accept(receiver, tracker, PN_CUMULATIVE);
    } 

  }

  async->completed = true;
  uv_async_send(&async->watcher);

}

void Messenger::AsyncReceive(uv_async_t* handle, int status) {
  HandleScope scope;
  Async* async = static_cast<Async*>(handle->data);

  while (true) {

    Messages messages;
    NODE_CPROTON_MUTEX_LOCK(&async->mutex)
    messages.swap(async->data);
    NODE_CPROTON_MUTEX_UNLOCK(&async->mutex)

    if (messages.empty()) {
      break;
    }

    Local<Value> argv[2];

    Messages::const_iterator it = messages.begin();
    Messages::const_iterator end = messages.end();
    for (int i = 0; it < end; it++, i++) {

      argv[0] = String::NewSymbol("message");
      argv[1] = MessageToJS(*it);
      TRY_CATCH_CALL(async->msgr->handle_, async->emitter, 2, argv)
      pn_message_free(*it);
      // delete *it;

    }

  }

  if (async->completed) {
    uv_close((uv_handle_t*)handle, CloseEmitter);
  }

}

void Messenger::Work_AfterReceive(uv_work_t* req) {

  HandleScope scope;
  SendBaton* baton = static_cast<SendBaton*>(req->data);

  delete baton;

}

Handle<Value> Messenger::Stop(const Arguments& args) {
  HandleScope scope;

  OPTIONAL_ARGUMENT_FUNCTION(0, callback);

  Messenger* msgr = ObjectWrap::Unwrap<Messenger>(args.This());

  Baton* baton = new Baton(msgr, callback);

  if (baton->msgr->receiving || baton->msgr->receiveWait) {

    baton->msgr->receiving = false;
    baton->msgr->receiveWait = false;

    Work_BeginStop(baton);

  }

  return Undefined();

}

void Messenger::Work_BeginStop(Baton *baton) {

  int status = uv_queue_work(uv_default_loop(),
    &baton->request, Work_Stop, (uv_after_work_cb)Work_AfterStop);

  assert(status == 0);

}

void Messenger::Work_Stop(uv_work_t* req) {

  Baton* baton = static_cast<Baton*>(req->data);

  baton->msgr->receiving = false;

}

void Messenger::Work_AfterStop(uv_work_t* req) {

}

pn_message_t* Messenger::JSToMessage(Local<Object> obj) {

  // TODO:  Should have a pool of these and re-use them
  pn_message_t* message = pn_message();

  if (obj->Has(String::New("address"))) {

    String::Utf8Value addr(obj->Get(String::New("address")));
    char * str_addr = *addr;

    pn_message_set_address(message, str_addr);

  }

  if (obj->Has(String::New("body"))) {

    String::Utf8Value body(obj->Get(String::New("body")));
    char * str_body = *body;

    pn_data_t* msg_body = pn_message_body(message);
    pn_data_put_string(msg_body, pn_bytes(body.length(), str_body));

  }

  return(message);

}

Local<Object> Messenger::MessageToJS(pn_message_t* message) {

  Local<Object> result(Object::New());

  size_t buffsize = 1024;
  char buffer[1024];
  pn_data_t *body = pn_message_body(message);
  pn_data_format(body, buffer, &buffsize);

  result->Set(String::NewSymbol("body"), Local<Value>(String::New(buffer)));

  return result;

}
