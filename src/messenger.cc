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
    pn_data_put_decimal32(data, val);    
  } else if(type.compare("DECIMAL64") == 0) {
    pn_decimal64_t val;
    istream >> val;
    pn_data_put_decimal64(data, val);    
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

pn_type_t Messenger::JSTypeToPNType(std::string type)
{
  pn_type_t rtype = PN_NULL;
  
  if(type.compare("NULL") == 0) {
    rtype = PN_NULL;
  } else if(type.compare("BOOL") == 0) {
    rtype = PN_BOOL;
  } else if(type.compare("UBYTE") == 0) {
    rtype = PN_UBYTE;
  } else if(type.compare("BYTE") == 0) {
    rtype = PN_BYTE;
  } else if(type.compare("USHORT") == 0) {
    rtype = PN_USHORT;
  } else if(type.compare("SHORT") == 0) {
    rtype = PN_SHORT;
  } else if(type.compare("UINT") == 0) {
    rtype = PN_UINT;
  } else if(type.compare("INT") == 0) {
    rtype = PN_INT;
  } else if(type.compare("CHAR") == 0) {
    rtype = PN_CHAR;
  } else if(type.compare("ULONG") == 0) {
    rtype = PN_ULONG;
  } else if(type.compare("LONG") == 0) {
    rtype = PN_LONG;
  } else if(type.compare("TIMESTAMP") == 0) {
    rtype = PN_TIMESTAMP;
  } else if(type.compare("FLOAT") == 0) {
    rtype = PN_FLOAT;
  } else if(type.compare("DOUBLE") == 0) {
    rtype = PN_DOUBLE;
  } else if(type.compare("DECIMAL32") == 0) {
    rtype = PN_DECIMAL32;
  } else if(type.compare("DECIMAL64") == 0) {
    rtype = PN_DECIMAL64;
  } else if(type.compare("DECIMAL128") == 0) {
    rtype = PN_DECIMAL128;
  } else if(type.compare("UUID") == 0) {
    rtype = PN_UUID;
  } else if(type.compare("BINARY") == 0) {
    rtype = PN_BINARY;
  } else if(type.compare("STRING") == 0) {
    rtype = PN_STRING;
  } else if(type.compare("SYMBOL") == 0) {
    rtype = PN_SYMBOL;
  } else if(type.compare("DESCRIBED") == 0) {
    rtype = PN_DESCRIBED;
  } else if(type.compare("ARRAY") == 0) {
    rtype = PN_ARRAY;
  } else if(type.compare("LIST") == 0) {
    rtype = PN_LIST;
  } else if(type.compare("MAP") == 0) {
    rtype = PN_MAP;
  }
  
  return rtype;    
}

std::string Messenger::PNTypeToJSType(pn_type_t type)
{
  std::string rtype;
  
  switch(type)
  {
    case PN_NULL:
      rtype = std::string("NULL");
      break;
    case PN_BOOL:
      rtype = std::string("BOOL");
      break;
    case PN_UBYTE:
      rtype = std::string("UBYTE");
      break;
    case PN_BYTE:
      rtype = std::string("BYTE");
      break;
    case PN_USHORT:
      rtype = std::string("USHORT");
      break;
    case PN_SHORT:
      rtype = std::string("SHORT");
      break;
    case PN_UINT:
      rtype = std::string("UINT");
      break;
    case PN_INT:
      rtype = std::string("INT");
      break;
    case PN_CHAR:
      rtype = std::string("CHAR");
      break;
    case PN_ULONG:
      rtype = std::string("ULONG");
      break;
    case PN_LONG:
      rtype = std::string("LONG");
      break;
    case PN_TIMESTAMP:
      rtype = std::string("TIMESTAMP");
      break;
    case PN_FLOAT:
      rtype = std::string("FLOAT");
      break;
    case PN_DOUBLE:
      rtype = std::string("DOUBLE");
      break;
    case PN_DECIMAL32:
      rtype = std::string("DECIMAL32");
      break;
    case PN_DECIMAL64:
      rtype = std::string("DECIMAL64");
      break;
    case PN_DECIMAL128:
      rtype = std::string("DECIMAL128");
      break;
    case PN_UUID:
      rtype = std::string("UUID");
      break;
    case PN_BINARY:
      rtype = std::string("BINARY");
      break;
    case PN_STRING:
      rtype = std::string("STRING");
      break;
    case PN_SYMBOL:
      rtype = std::string("SYMBOL");
      break;
    case PN_DESCRIBED:
      rtype = std::string("DESCRIBED");
      break;
    case PN_ARRAY:
      rtype = std::string("ARRAY");
      break;
    case PN_LIST:
      rtype = std::string("LIST");
      break;
    case PN_MAP:
      rtype = std::string("MAP");
      break;
  }
  
  return rtype;    
}

bool Messenger::IsSimpleValue(pn_type_t type)
{
  bool rval = true;
  switch(type)
  {
    case PN_DESCRIBED:
    case PN_ARRAY:
    case PN_LIST:
    case PN_MAP:
      rval = false;
      break;
    default:
      ;
  }
  return rval;
}

Handle<Value> Messenger::GetSimpleValue(pn_data_t *data)
{
  HandleScope scope;
  
  switch(pn_data_type(data))
  {
    case PN_NULL:
      return Boolean::New(false);
    case PN_BOOL:
      return Boolean::New(pn_data_get_bool(data));
    case PN_UBYTE:
      return Integer::NewFromUnsigned(pn_data_get_ubyte(data));
    case PN_BYTE:
      return Integer::New(pn_data_get_byte(data));
    case PN_USHORT:
      return Integer::NewFromUnsigned(pn_data_get_short(data));
    case PN_SHORT:
      return Integer::New(pn_data_get_short(data));
    case PN_UINT:
      return Integer::NewFromUnsigned(pn_data_get_uint(data));
    case PN_INT:
      return Integer::New(pn_data_get_int(data));
    case PN_CHAR:
      return Integer::New(pn_data_get_char(data));
    case PN_ULONG:
      return Number::New(pn_data_get_ulong(data));
    case PN_LONG:
      return Number::New(pn_data_get_long(data));
    case PN_TIMESTAMP:
      return Number::New(pn_data_get_timestamp(data));
    case PN_FLOAT:
      return Number::New(pn_data_get_float(data));
    case PN_DOUBLE:
      return Number::New(pn_data_get_double(data));
    case PN_DECIMAL32:
      return Number::New(pn_data_get_decimal32(data));
    case PN_DECIMAL64:
      return Number::New(pn_data_get_decimal64(data));
    case PN_DECIMAL128:
      {
        pn_decimal128_t d128 = pn_data_get_decimal128(data);
        return String::New(d128.bytes, 16);
      }
    case PN_UUID:
      {
        pn_uuid_t puu = pn_data_get_uuid(data);
        return String::New(puu.bytes, 16);
      }
    case PN_BINARY:
    case PN_STRING:
    case PN_SYMBOL:
      {
        pn_bytes_t b;
        if(pn_data_type(data) == PN_BINARY) {
          b =  pn_data_get_binary(data);
        } else if(pn_data_type(data) == PN_STRING) {      
          b = pn_data_get_string(data);
        } else {
          b = pn_data_get_symbol(data);
        }
        return String::New(b.start, b.size);
      }
    default:
      ;
  }
  return Null(); 
}

Handle<Value> Messenger::GetDescribedValue(pn_data_t *data)
{
  HandleScope scope;
  
  pn_data_enter(data);
  Handle<Value> description = Messenger::ParsePnData(data);
  pn_data_next(data);
  Handle<Value> value = Messenger::ParsePnData(data);
  pn_data_exit(data);
  
  Handle<Object> t(Object::New());
  t->Set(String::New("description"), description);
  t->Set(String::New("value"), value);
  
  return t;
}

Handle<Value> Messenger::GetArrayValue(pn_data_t *data)
{
  HandleScope scope;
  
  Handle<Array> t = Array::New();
  int idx = 0;
  pn_data_enter(data);
  while(pn_data_next(data)) {
    t->Set(idx++, Messenger::ParsePnData(data));
  }
  
  return t;
}

Handle<Value> Messenger::GetListValue(pn_data_t *data)
{
  return Messenger::GetArrayValue(data);
}

Handle<Value> Messenger::GetMapValue(pn_data_t *data)
{
  HandleScope scope;
  
  Handle<Object> t(Array::New());

  int nodes = pn_data_get_map(data);
  if(nodes % 2) {
    return Null();
  } else {
    pn_data_enter(data);
    for(int i = 0; i < nodes / 2; i++) {
      t->Set(2*i, ParsePnData(data));
      t->Set(2*i+1, ParsePnData(data));
    }
  }
  
  return t;
}

Handle<Value> Messenger::ParseValue(pn_data_t *data)
{
  if(Messenger::IsSimpleValue(pn_data_type(data))) {
    return GetSimpleValue(data);
  } else if(pn_data_type(data) == PN_DESCRIBED) {
    return GetDescribedValue(data);
  } else if(pn_data_type(data) == PN_ARRAY) {
    return GetArrayValue(data);
  } else if(pn_data_type(data) == PN_ARRAY) {
    return GetListValue(data);
  } else if(pn_data_type(data) == PN_MAP) {
    return GetMapValue(data);
  }
  return Null();
}

Local<Array> Messenger::ParsePnData(pn_data_t *data)
{
  HandleScope scope;
 /* 
  Local<Object> t(Object::New());
  
  pn_data_next(data);
  t->Set(String::New("type"), String::New(PNTypeToJSType(pn_data_type(data)).c_str()));
  t->Set(String::New("value"), ParseValue(data));
  */
  Local<Array> t = Array::New();
  pn_data_next(data);
  t->Set(0, String::New(PNTypeToJSType(pn_data_type(data)).c_str()));
  t->Set(1, ParseValue(data));
  return scope.Close(t);
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

  // handle the body
  pn_data_t *body = pn_message_body(message);
  pn_data_format(body, buffer, &buffsize);
  result->Set(String::NewSymbol("body"), Local<Value>(String::New(buffer)));
  
  // check for annotations
  pn_data_t *annotations = pn_message_annotations(message);
  if(annotations && pn_data_size(annotations) > 0) {
    Handle<Value> anno = Messenger::ParsePnData(annotations);
    result->Set(String::NewSymbol("annotations"), anno);
  }

  // check for properties
  pn_data_t *properties = pn_message_properties(message);
  if(properties && pn_data_size(properties) > 0) {
    Handle<Value> prop = Messenger::ParsePnData(properties);
    result->Set(String::NewSymbol("properties"), prop);
  }
  
  return result;
}
