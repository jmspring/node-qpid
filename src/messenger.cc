#define BUILDING_NODE_EXTENSION
#include <node.h>
#include <iostream>
#include <vector>
#include <string>
#include <algorithm>
#include <sstream>
#include <map>

#include <string.h>
#include <limits.h>
#include <uuid/uuid.h>

#include "messenger.h"
#include "async.h"


using namespace v8;
using namespace node;
using namespace std;

Messenger::Messenger() { 
  NODE_CPROTON_MUTEX_INIT(mutex);
};

Messenger::~Messenger() {
  pn_messenger_stop(messenger);
  pn_messenger_stop(receiver);
  NODE_CPROTON_MUTEX_DESTROY(mutex);
}

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

Messenger::Subscription *Messenger::GetSubscriptionByAddress(std::string addr) {
  Subscription *rval = NULL;
  std::map<std::string, unsigned long>::iterator it = _addressToSubscriptionMap.find(addr);
  if(it != _addressToSubscriptionMap.end()) {
    if(it->second < _subscriptions.size()) {
      rval = _subscriptions.at(it->second);
    }
  }
  return rval;
}

Messenger::Subscription *Messenger::GetSubscriptionByIndex(unsigned long idx) {
  Subscription *rval = NULL;
  if(idx < _subscriptions.size()) {
    rval = _subscriptions.at(idx);
  }
  return rval;
}

Messenger::Subscription *Messenger::GetSubscriptionByHandle(pn_subscription_t *sub) {
  Subscription *rval = NULL;
  std::map<pn_subscription_t *, unsigned long>::iterator it = _handleToSubscriptionMap.find(sub);
  if(it != _handleToSubscriptionMap.end()) {
    if(it->second < _subscriptions.size()) {
      rval = _subscriptions.at(it->second);
    }
  }
  return rval;
}

unsigned long Messenger::AddSubscription(Messenger::Subscription *sub) {
  unsigned long idx = ULONG_MAX;
  if(sub) {
    _subscriptions.push_back(sub);
    idx = _subscriptions.size() - 1;
    _addressToSubscriptionMap.insert( std::pair<std::string, unsigned long>( sub->address, idx ));
  }
  return idx;
}

bool Messenger::SetSubscriptionHandle(unsigned long idx, pn_subscription_t *sub) {
  bool rval = false;
  if(sub) {
    std::map<pn_subscription_t *, unsigned long>::iterator it = _handleToSubscriptionMap.find(sub);
    if(it == _handleToSubscriptionMap.end()) {
      _handleToSubscriptionMap.insert( std::pair<pn_subscription_t *, unsigned long>( sub, idx ));
      rval = true;
    }
  }
  return rval;
}

Handle<Value> Messenger::Subscribe(const Arguments& args) {
  HandleScope scope;

  Messenger* msgr = ObjectWrap::Unwrap<Messenger>(args.This());

  REQUIRE_ARGUMENT_STRING(0, addr);
  REQUIRE_ARGUMENT_OBJECT(1, bag);
  OPTIONAL_ARGUMENT_FUNCTION(2, callback);
  
  
  pn_data_t *filter_key = NULL, *filter_value = NULL;
  if(bag->Has(String::New("sourceFilter"))) {
    Handle<Value> obj = bag->Get(String::New("sourceFilter"));
    if(obj->IsArray()) {
      Handle<Array> filter = Handle<v8::Array>::Cast(obj);
      if(filter->Length() == 2) {
        filter_key = ParseJSData(filter->Get(0));
        filter_value = ParseJSData(filter->Get(1));
        
        if(!filter_key || !filter_value) {
          // TODO -- error;
          if(filter_key) {
            pn_data_free(filter_key);
          }
          if(filter_value) {
            pn_data_free(filter_value);
          }
          filter_key = filter_value = NULL;
        }
      }
    } else {
      // TODO -- unsupported
    }
  }
  
  NODE_CPROTON_MUTEX_LOCK(&msgr->mutex);
  int subIdx = msgr->AddSubscription(new Subscription(*addr, callback));  
  NODE_CPROTON_MUTEX_UNLOCK(&msgr->mutex);
  Local<Function> var;
  SubscribeBaton *baton = new SubscribeBaton(msgr, subIdx, filter_key, filter_value, var);
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

  NODE_CPROTON_MUTEX_LOCK(&baton->msgr->mutex)
  Subscription *subscription = baton->msgr->GetSubscriptionByIndex(baton->subscriptionIndex);
  if(subscription != NULL) {
    pn_subscription_t * sub = pn_messenger_subscribe(baton->msgr->receiver, subscription->address.c_str());
    if(sub) {
      baton->msgr->SetSubscriptionHandle(baton->subscriptionIndex, sub);
      if(baton->filter_key && baton->filter_value) {
        baton->msgr->SetSourceFilter(subscription->address, baton->filter_key, baton->filter_value);
      }
    } else {
      // TODO -- error getting subscription?
    }
  } else {
    // TODO -- something bad happened
  }
  NODE_CPROTON_MUTEX_UNLOCK(&baton->msgr->mutex)
}

void Messenger::Work_AfterSubscribe(uv_work_t* req) {
  HandleScope scope;

  SubscribeBaton* baton = static_cast<SubscribeBaton*>(req->data);
  
  NODE_CPROTON_MUTEX_LOCK(&baton->msgr->mutex)
  Subscription *subscription = baton->msgr->GetSubscriptionByIndex(baton->subscriptionIndex);
  if(subscription) {
    if(!subscription->callback.IsEmpty() && subscription->callback->IsFunction()) {
      Local<Value> args[] = { String::New(subscription->address.c_str()) }; 
      subscription->callback->Call(Context::GetCurrent()->Global(), 1, args);
    } else {
      Local<Value> args[] = { String::New("subscribed"), String::New(subscription->address.c_str()) };
      EMIT_EVENT(baton->msgr->handle_, 2, args);
    }
  }
  baton->msgr->subscriptions++;
  NODE_CPROTON_MUTEX_UNLOCK(&baton->msgr->mutex)

#if 0
// TODO -- add error handling???
  if (baton->error_code > 0) {
    /* Local<Value> err = Exception::Error(
    String::New(baton->error_message.c_str()));
    Local<Value> argv[2] = { Local<Value>::New(String::New("error")), err };
    MakeCallback(baton->obj, "emit", 2, argv); */
  } 
#endif   

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
  
  if(type.compare("null") == 0) {
    pn_data_put_null(data);
  } else if(type.compare("bool") == 0) {
    bool val = lowercase(value).compare("false");
    pn_data_put_bool(data, val);    
  } else if(type.compare("ubyte") == 0) {
    uint8_t val;
    istream >> val;
    pn_data_put_ubyte(data, val);    
  } else if(type.compare("byte") == 0) {
    int8_t val;
    istream >> val;
    pn_data_put_byte(data, val);    
  } else if(type.compare("ushort") == 0) {
    uint16_t val;
    istream >> val;
    pn_data_put_ushort(data, val);    
  } else if(type.compare("short") == 0) {
    int16_t val;
    istream >> val;
    pn_data_put_short(data, val);    
  } else if(type.compare("uint") == 0) {
    uint32_t val;
    istream >> val;
    pn_data_put_uint(data, val);    
  } else if(type.compare("int") == 0) {
    int32_t val;
    istream >> val;
    pn_data_put_int(data, val);    
  } else if(type.compare("char") == 0) {
    pn_char_t val;
    istream >> val;
    pn_data_put_char(data, val);    
  } else if(type.compare("ulong") == 0) {
    uint64_t val;
    istream >> val;
    pn_data_put_ulong(data, val);    
  } else if(type.compare("long") == 0) {
    int64_t val;
    istream >> val;
    pn_data_put_long(data, val);    
  } else if(type.compare("timestamp") == 0) {
    pn_timestamp_t val;
    istream >> val;
    pn_data_put_timestamp(data, val);    
  } else if(type.compare("float") == 0) {
    float val;
    istream >> val;
    pn_data_put_float(data, val);    
  } else if(type.compare("double") == 0) {
    double val;
    istream >> val;
    pn_data_put_double(data, val);    
  } else if(type.compare("decimal32") == 0) {
    pn_decimal32_t val;
    istream >> val;
    pn_data_put_decimal32(data, val);    
  } else if(type.compare("decimal64") == 0) {
    pn_decimal64_t val;
    istream >> val;
    pn_data_put_decimal64(data, val);    
  } else if(type.compare("decimal128") == 0) {
    pn_data_free(data);
    data = NULL;   
  } else if(type.compare("uuid") == 0) {
    uuid_t uu;
    pn_uuid_t puu;
    uuid_parse(value.c_str(), uu);
    memcpy(puu.bytes, uu, 16);
    pn_data_put_uuid(data, puu);    
  } else if(type.compare("binary") == 0) {
    pn_data_free(data);
    data = NULL;
  } else if(type.compare("string") == 0) {
    pn_bytes_t val = { value.length(), value.c_str() };
    pn_data_put_string(data, val);    
  } else if(type.compare("symbol") == 0) {
    pn_bytes_t val = { value.length(), value.c_str() };
    pn_data_put_symbol(data, val);    
  } else if(type.compare("described") == 0) {
    // currently we only support string for described data
    pn_data_put_described(data);
    pn_data_enter(data);
    pn_bytes_t desc = { described.length(), described.c_str() };
    pn_bytes_t val = { value.length(), value.c_str() };
    pn_data_put_symbol(data, desc);
    pn_data_put_string(data, val);
    pn_data_exit(data);
    pn_data_rewind(data);
  } else if(type.compare("array") == 0) {
    pn_data_free(data);
    data = NULL;
  } else if(type.compare("list") == 0) {
    pn_data_free(data);
    data = NULL;
  } else if(type.compare("map") == 0) {
    pn_data_free(data);
    data = NULL;
  }
    
  return data;  
}

pn_type_t Messenger::JSTypeToPNType(std::string type)
{
  pn_type_t rtype = PN_NULL;
  
  if(type.compare("null") == 0) {
    rtype = PN_NULL;
  } else if(type.compare("bool") == 0) {
    rtype = PN_BOOL;
  } else if(type.compare("ubyte") == 0) {
    rtype = PN_UBYTE;
  } else if(type.compare("byte") == 0) {
    rtype = PN_BYTE;
  } else if(type.compare("ushort") == 0) {
    rtype = PN_USHORT;
  } else if(type.compare("short") == 0) {
    rtype = PN_SHORT;
  } else if(type.compare("uint") == 0) {
    rtype = PN_UINT;
  } else if(type.compare("int") == 0) {
    rtype = PN_INT;
  } else if(type.compare("char") == 0) {
    rtype = PN_CHAR;
  } else if(type.compare("ulong") == 0) {
    rtype = PN_ULONG;
  } else if(type.compare("long") == 0) {
    rtype = PN_LONG;
  } else if(type.compare("timestamp") == 0) {
    rtype = PN_TIMESTAMP;
  } else if(type.compare("float") == 0) {
    rtype = PN_FLOAT;
  } else if(type.compare("double") == 0) {
    rtype = PN_DOUBLE;
  } else if(type.compare("decimal32") == 0) {
    rtype = PN_DECIMAL32;
  } else if(type.compare("decimal64") == 0) {
    rtype = PN_DECIMAL64;
  } else if(type.compare("decimal128") == 0) {
    rtype = PN_DECIMAL128;
  } else if(type.compare("uuid") == 0) {
    rtype = PN_UUID;
  } else if(type.compare("binary") == 0) {
    rtype = PN_BINARY;
  } else if(type.compare("string") == 0) {
    rtype = PN_STRING;
  } else if(type.compare("symbol") == 0) {
    rtype = PN_SYMBOL;
  } else if(type.compare("described") == 0) {
    rtype = PN_DESCRIBED;
  } else if(type.compare("array") == 0) {
    rtype = PN_ARRAY;
  } else if(type.compare("list") == 0) {
    rtype = PN_LIST;
  } else if(type.compare("map") == 0) {
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
      rtype = std::string("null");
      break;
    case PN_BOOL:
      rtype = std::string("bool");
      break;
    case PN_UBYTE:
      rtype = std::string("ubyte");
      break;
    case PN_BYTE:
      rtype = std::string("byte");
      break;
    case PN_USHORT:
      rtype = std::string("ushort");
      break;
    case PN_SHORT:
      rtype = std::string("short");
      break;
    case PN_UINT:
      rtype = std::string("uint");
      break;
    case PN_INT:
      rtype = std::string("int");
      break;
    case PN_CHAR:
      rtype = std::string("char");
      break;
    case PN_ULONG:
      rtype = std::string("ulong");
      break;
    case PN_LONG:
      rtype = std::string("long");
      break;
    case PN_TIMESTAMP:
      rtype = std::string("timestamp");
      break;
    case PN_FLOAT:
      rtype = std::string("float");
      break;
    case PN_DOUBLE:
      rtype = std::string("double");
      break;
    case PN_DECIMAL32:
      rtype = std::string("decimal32");
      break;
    case PN_DECIMAL64:
      rtype = std::string("decimal64");
      break;
    case PN_DECIMAL128:
      rtype = std::string("decimal128");
      break;
    case PN_UUID:
      rtype = std::string("uuid");
      break;
    case PN_BINARY:
      rtype = std::string("binary");
      break;
    case PN_STRING:
      rtype = std::string("string");
      break;
    case PN_SYMBOL:
      rtype = std::string("symbol");
      break;
    case PN_DESCRIBED:
      rtype = std::string("described");
      break;
    case PN_ARRAY:
      rtype = std::string("array");
      break;
    case PN_LIST:
      rtype = std::string("list");
      break;
    case PN_MAP:
      rtype = std::string("map");
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
  } else if(pn_data_type(data) == PN_LIST) {
    return GetListValue(data);
  } else if(pn_data_type(data) == PN_MAP) {
    return GetMapValue(data);
  }
  return Null();
}

Local<Array> Messenger::ParsePnData(pn_data_t *data)
{
  HandleScope scope;
  Local<Array> t = Array::New();
  pn_data_next(data);
  t->Set(0, String::New(PNTypeToJSType(pn_data_type(data)).c_str()));
  t->Set(1, ParseValue(data));
  return scope.Close(t);
}

pn_data_t *Messenger::GetSimpleJSValue(pn_type_t type, Local<Value> jsval)
{
  pn_data_t *rval = pn_data(0);
  
  switch(type)
  {
    case PN_NULL:
      pn_data_put_null(rval);
      break;
    case PN_BOOL:
      pn_data_put_bool(rval, jsval->ToBoolean()->Value());
      break;
    case PN_UBYTE:
      pn_data_put_ubyte(rval, jsval->ToInteger()->Value());
      break;
    case PN_BYTE:
      pn_data_put_byte(rval, jsval->ToInteger()->Value());
      break;
    case PN_USHORT:
      pn_data_put_ushort(rval, jsval->ToInteger()->Value());
      break;
    case PN_SHORT:
      pn_data_put_short(rval, jsval->ToInteger()->Value());
      break;
    case PN_UINT:
      pn_data_put_uint(rval, jsval->ToUint32()->Value());
      break;
    case PN_INT:
      pn_data_put_int(rval, jsval->ToInteger()->Value());
      break;
    case PN_CHAR:
      pn_data_put_char(rval, jsval->ToInteger()->Value());
      break;
    case PN_ULONG:
      pn_data_put_ulong(rval, jsval->ToNumber()->Value());
      break;
    case PN_LONG:
      pn_data_put_long(rval, jsval->ToNumber()->Value());
      break;
    case PN_TIMESTAMP:
      pn_data_put_timestamp(rval, jsval->ToNumber()->Value());
      break;
    case PN_FLOAT:
      pn_data_put_float(rval, jsval->ToNumber()->Value());
      break;
    case PN_DOUBLE:
      pn_data_put_double(rval, jsval->ToNumber()->Value());
      break;
    case PN_DECIMAL32:
      pn_data_put_decimal32(rval, jsval->ToInt32()->Value());
      break;
    case PN_DECIMAL64:
      pn_data_put_decimal64(rval, jsval->ToNumber()->Value());
      break;
    case PN_DECIMAL128:
      {
        pn_decimal128_t d128;
        if(jsval->ToString()->Utf8Length() != 16) {
          pn_data_free(rval);
          rval = NULL;
        } else {
          jsval->ToString()->WriteUtf8(d128.bytes, 16);
          pn_data_put_decimal128(rval, d128);
        }
      }
      break;
    case PN_UUID:
      {
        pn_uuid_t puu;
        if(jsval->ToString()->Utf8Length() != 16) {
          pn_data_free(rval);
          rval = NULL;
        } else {
          jsval->ToString()->WriteUtf8(puu.bytes, 16);
          pn_data_put_uuid(rval, puu);
        }
      }
      break;
    case PN_BINARY:
    case PN_STRING:
    case PN_SYMBOL:
      {
        pn_bytes_t b;
        String::Utf8Value valstr(jsval->ToString());
        b.start = *valstr;
        b.size = valstr.length();
        if(type == PN_BINARY) {
          pn_data_put_binary(rval, b);
        } else if(type == PN_STRING) {      
          pn_data_put_string(rval, b);
        } else {
          pn_data_put_symbol(rval, b);
        }
      }
    default:
      ;
  }
  
  return rval;
}

pn_data_t *Messenger::GetDescribedJSValue(Handle<Array> array)
{
  pn_data_t *rval = NULL;
  
  if(array->Length() == 3) {  
    pn_data_t *description = ParseJSData(array->Get(1));
    if(description) {
      pn_data_t *value = ParseJSData(array->Get(2));
      if(value) {
        rval = pn_data(0);
        pn_data_put_described(rval);
        pn_data_enter(rval);
        pn_data_append(rval, description);
        pn_data_append(rval, value);
        pn_data_exit(rval);
        pn_data_rewind(rval);
      }
      pn_data_free(value);
    }
    pn_data_free(description);
  } else {
    // TODO - failure scenario
  }
  
  return rval;
}

pn_data_t *Messenger::GetArrayOrListJSValue(pn_type_t type, Handle<Array> array)
{
  pn_data_t *rval = pn_data(0);
  pn_type_t arraytype = PN_NULL;
  
  if(type == PN_LIST) {
    pn_data_put_list(rval);
  } else {
    if(array->Length() > 0) {
      pn_data_t *data = ParseJSData(array->Get(0));
      if(data) {
        arraytype = pn_data_type(data);
        pn_data_free(data);      
      } else {
        // TODO - handle error;
        pn_data_free(rval);
        return NULL;
      }
    }
    pn_data_put_array(rval, false, arraytype);
  }
  
  pn_data_enter(rval);
  for(unsigned int i = 1; i < array->Length(); i++) {
    pn_data_t *data = ParseJSData(array->Get(i));
    if(data) {
      pn_data_append(rval, data);
    } else {
      // TODO - mention failure
    }
  }
  pn_data_exit(rval);
  pn_data_rewind(rval);
  
  return rval;
}

pn_data_t *Messenger::GetArrayJSValue(Handle<Array> array)
{
  return GetArrayOrListJSValue(PN_ARRAY, array);
}

pn_data_t *Messenger::GetListJSValue(Handle<Array> array)
{
  return GetArrayOrListJSValue(PN_LIST, array);
}

pn_data_t *Messenger::GetMapJSValue(Handle<Array> array)
{
  pn_data_t *rval = NULL;
  
  if(array->Length() % 2 == 0) {
    rval = pn_data(0);
    pn_data_put_map(rval);
    pn_data_enter(rval);
    unsigned int nodecnt = array->Length() / 2;
    for(unsigned int i = 0; i < nodecnt; i++) {
      pn_data_t *key = ParseJSData(array->Get(i * 2));
      if(key) {
        pn_data_t *value = ParseJSData(array->Get(i * 2 + 1));
        if(value) {
          pn_data_append(rval, key);
          pn_data_append(rval, value);
        } else {
          // TODO - log failure
          pn_data_free(key);
        }
      } else {
        // TODO - log failure
      }
    }
    pn_data_exit(rval);
    pn_data_rewind(rval);
  } else {
    // TODO - invalid number of entries
  }
  
  return rval;
}

pn_data_t *Messenger::ParseJSData(Handle<Value> jsval)
{
  pn_data_t *rval = NULL;
  
  if(jsval->IsArray()) {
    Handle<Array> array = Handle<v8::Array>::Cast(jsval);
  
    String::Utf8Value typestr(array->Get(0)->ToString());
    pn_type_t type = JSTypeToPNType(std::string(*typestr));
    if(IsSimpleValue(type)) {
      rval = GetSimpleJSValue(type, array->Get(1));
    } else if(type == PN_DESCRIBED) {
      rval = GetDescribedJSValue(array);
    } else {
      Handle<Array> value = Handle<Array>::Cast(array->Get(1));
      if(type == PN_ARRAY) {
        rval = GetArrayJSValue(value);
      } else if(type == PN_LIST) {
        rval = GetListJSValue(value);
      } else if(type == PN_MAP) {
        rval = GetMapJSValue(value);
      } else {
        // TODO -- failure?
        return NULL;
      }
    }
  }
  return rval;
}


Handle<Value> Messenger::AddSourceFilter(const Arguments& args) {
  HandleScope scope;

  Messenger* msgr = ObjectWrap::Unwrap<Messenger>(args.This());

  REQUIRE_ARGUMENT_STRING(0, addr);
  REQUIRE_ARGUMENT_ARRAY(1, filter);
  OPTIONAL_ARGUMENT_FUNCTION(2, callback);
  
  pn_data_t *key = ParseJSData(filter->Get(0));
  pn_data_t *value = ParseJSData(filter->Get(1));

  if(key && value) {
    AddSourceFilterBaton *baton = new AddSourceFilterBaton(msgr, callback, *addr, key, value);
    Work_BeginAddSourceFilter(baton);
  }

  return args.This();
}

void Messenger::Work_BeginAddSourceFilter(Baton* baton) {
  int status = uv_queue_work(uv_default_loop(),
    &baton->request, Work_AddSourceFilter, (uv_after_work_cb)Work_AfterAddSourceFilter);

  assert(status == 0);

}

void Messenger::SetSourceFilter(std::string & address, pn_data_t *key, pn_data_t *value) {
  pn_link_t *link = pn_messenger_get_link(this->receiver, address.c_str(), 0);
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
        pn_data_append(filter, key);
        pn_data_append(filter, value);
        pn_data_exit(filter);
        pn_data_rewind(filter);
      }
    }
  }
}

void Messenger::Work_AddSourceFilter(uv_work_t* req) {

  AddSourceFilterBaton* baton = static_cast<AddSourceFilterBaton*>(req->data);
  
  // TODO - add check for subscribed to address
  NODE_CPROTON_MUTEX_LOCK(&baton->msgr->mutex)
  baton->msgr->SetSourceFilter(baton->address, baton->filter_key, baton->filter_value);
  NODE_CPROTON_MUTEX_UNLOCK(&baton->msgr->mutex)

  /*
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
        pn_data_append(filter, baton->filter_key);
        pn_data_append(filter, baton->filter_value);
        pn_data_exit(filter);
        pn_data_rewind(filter);
      }
    }
  }
  */
}

void Messenger::Work_AfterAddSourceFilter(uv_work_t* req) {

  AddSourceFilterBaton* baton = static_cast<AddSourceFilterBaton*>(req->data);
  
  if(baton->filter_value) {
    pn_data_free(baton->filter_value);
    baton->filter_value = NULL;
  }

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
      pn_subscription_t *subscription = pn_messenger_incoming_subscription(receiver);
      
      MessageInfo *mi = new MessageInfo();
      mi->message = message;

      NODE_CPROTON_MUTEX_LOCK(&async->mutex)
      Messenger::Subscription *sub = baton->msgr->GetSubscriptionByHandle(subscription);
      if(sub) {
        mi->subscription_address = sub->address;
      }
      async->data.push_back(mi);
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

    Local<Value> argv[3];

    Messages::const_iterator it = messages.begin();
    Messages::const_iterator end = messages.end();
    for (int i = 0; it < end; it++, i++) {

      argv[0] = String::NewSymbol("message");
      argv[1] = MessageToJS((*it)->message);
      argv[2] = String::New((*it)->subscription_address.c_str());
      TRY_CATCH_CALL(async->msgr->handle_, async->emitter, 3, argv)
      pn_message_free((*it)->message);
      delete (*it);
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

