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
  messageSender = new MessageSender(this);
  messageSettler = new MessageSettler(this);
};

Messenger::~Messenger() {
  pn_messenger_stop(messenger);
  pn_messenger_stop(receiver);
  delete(messageSender);
  delete(messageSettler);
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
  pn_messenger_set_blocking(messenger, false);
  pn_messenger_start(messenger);
  msgr->messenger = messenger;

  // Temporary fix -- should tune this
  pn_messenger_set_outgoing_window(msgr->messenger, 50);

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
  
  pn_data_t *filter_value = NULL;
  if(bag->Has(String::New("sourceFilter"))) {
    Handle<Value> obj = bag->Get(String::New("sourceFilter"));
    if(obj->IsArray()) {
      filter_value = ProtonData::ParseJSData(obj);
    } else {
      // TODO -- unsupported
    }
  }
  
  NODE_CPROTON_MUTEX_LOCK(&msgr->mutex);
  int subIdx = msgr->AddSubscription(new Subscription(*addr, callback));  
  NODE_CPROTON_MUTEX_UNLOCK(&msgr->mutex);
  Local<Function> var;
  SubscribeBaton *baton = new SubscribeBaton(msgr, subIdx, filter_value, var);
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
      if(baton->filter_value) {
        baton->msgr->SetSourceFilter(subscription->address, baton->filter_value);
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




Handle<Value> Messenger::AddSourceFilter(const Arguments& args) {
  HandleScope scope;

  Messenger* msgr = ObjectWrap::Unwrap<Messenger>(args.This());

  REQUIRE_ARGUMENT_STRING(0, addr);
  REQUIRE_ARGUMENT_OBJECT(1, filter);
  OPTIONAL_ARGUMENT_FUNCTION(2, callback);
  
  pn_data_t *value = ProtonData::ParseJSData(filter);

  if(value) {
    AddSourceFilterBaton *baton = new AddSourceFilterBaton(msgr, callback, *addr, value);
    Work_BeginAddSourceFilter(baton);
  }

  return args.This();
}

void Messenger::Work_BeginAddSourceFilter(Baton* baton) {
  int status = uv_queue_work(uv_default_loop(),
    &baton->request, Work_AddSourceFilter, (uv_after_work_cb)Work_AfterAddSourceFilter);

  assert(status == 0);

}

void Messenger::SetSourceFilter(std::string & address, pn_data_t *filter_value) {
  if(filter_value) {
    pn_link_t *link = pn_messenger_get_link(this->receiver, address.c_str(), 0);
    if(link) {
      pn_terminus_t *sr = pn_link_source(link);
      if(sr) {
        pn_data_t *filter = pn_terminus_filter(sr);
        if(filter) {
          pn_data_copy(filter, filter_value);
        }
      }
    }
  }
}

void Messenger::Work_AddSourceFilter(uv_work_t* req) {

  AddSourceFilterBaton* baton = static_cast<AddSourceFilterBaton*>(req->data);
  
  // TODO - add check for subscribed to address
  NODE_CPROTON_MUTEX_LOCK(&baton->msgr->mutex)
  baton->msgr->SetSourceFilter(baton->address, baton->filter_value);
  NODE_CPROTON_MUTEX_UNLOCK(&baton->msgr->mutex)

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
  pn_message_t *msg = JSToMessage(obj);

  InFlightMessage *ifmsg = new InFlightMessage(obj, msg, callback);
  msgr->messageSender->AppendMessage(ifmsg);

  return Undefined();
}


void Messenger::Work_BeginSend(Baton* baton) {
  SendBaton * send_baton = static_cast<SendBaton *>(baton);
  
  int status = uv_queue_work(uv_default_loop(),
    &baton->request, Work_Send, (uv_after_work_cb)Work_AfterSend);

  assert(status == 0);
}

void Messenger::Work_Send(uv_work_t* req) {

  SendBaton* baton = static_cast<SendBaton*>(req->data);
  
}

void Messenger::Work_AfterSend(uv_work_t* req) {
  HandleScope scope;
  
  SendBaton* baton = static_cast<SendBaton*>(req->data);
  
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

    pn_messenger_recv(receiver, 150);

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
  
  if (obj->Has(String::New("annotations"))) {
  
    Handle<Value> annotations(obj->Get(String::New("annotations")));
    
    pn_data_t *anndata = ProtonData::ParseJSData(annotations);
    pn_data_t *msgann = pn_message_annotations(message);
    pn_data_copy(msgann, anndata);
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
    Handle<Value> anno = ProtonData::ParsePnData(annotations);
    result->Set(String::NewSymbol("annotations"), anno);
  }

  // check for properties
  pn_data_t *properties = pn_message_properties(message);
  if(properties && pn_data_size(properties) > 0) {
    Handle<Value> prop = ProtonData::ParsePnData(properties);
    result->Set(String::NewSymbol("properties"), prop);
  }
  
  return result;
}

int Messenger::MessengerGetOutgoingWindow(void)
{
  int result;
  NODE_CPROTON_MUTEX_LOCK(&mutex)
  result = pn_messenger_get_outgoing_window(messenger);
  NODE_CPROTON_MUTEX_UNLOCK(&mutex)
  return result;
}

int Messenger::MessengerGetOutgoing(void){
  int result;
  NODE_CPROTON_MUTEX_LOCK(&mutex)
  result = pn_messenger_outgoing(messenger);
  NODE_CPROTON_MUTEX_UNLOCK(&mutex)
  return result;
}

bool Messenger::MessengerGetBuffered(pn_tracker_t tracker){
  bool result;
  NODE_CPROTON_MUTEX_LOCK(&mutex)
  result = pn_messenger_buffered(messenger, tracker);
  NODE_CPROTON_MUTEX_UNLOCK(&mutex)
  return result;
}

pn_status_t Messenger::MessengerGetStatus(pn_tracker_t tracker){
  pn_status_t result;
  NODE_CPROTON_MUTEX_LOCK(&mutex)
  result = pn_messenger_status(messenger, tracker);
  NODE_CPROTON_MUTEX_UNLOCK(&mutex)
  return result;
}

int Messenger::MessengerSettleOutgoing(pn_tracker_t tracker){
  int result;
  NODE_CPROTON_MUTEX_LOCK(&mutex)
  result = pn_messenger_settle(messenger, tracker, 0);
  NODE_CPROTON_MUTEX_UNLOCK(&mutex)
  return result;
}

int Messenger::MessengerSend(){
  int result;
  NODE_CPROTON_MUTEX_LOCK(&mutex)
  result = pn_messenger_send(messenger, 0);
  NODE_CPROTON_MUTEX_UNLOCK(&mutex)
  return result;
}

int Messenger::MessengerWork(){
  int result;
  NODE_CPROTON_MUTEX_LOCK(&mutex)
  result = pn_messenger_work(messenger, 0);
  NODE_CPROTON_MUTEX_UNLOCK(&mutex)
  return result;
}

pn_tracker_t Messenger::MessengerGetOutgoingTracker(void){
  pn_tracker_t result;
  NODE_CPROTON_MUTEX_LOCK(&mutex)
  result = pn_messenger_outgoing_tracker(messenger);
  NODE_CPROTON_MUTEX_UNLOCK(&mutex)
  return result;
}

int Messenger::MessengerPut(pn_message_t *msg){
  int result;
  NODE_CPROTON_MUTEX_LOCK(&mutex)
  result = pn_messenger_put(messenger, msg);
  NODE_CPROTON_MUTEX_UNLOCK(&mutex)
  return result;
}



std::string Messenger::MapErrorToString(int err) {
  switch(err) {
    case MSG_ERROR_NONE:
      return std::string("none");
    case MSG_ERROR_INTERNAL:
      return std::string("internal error");
    case MSG_ERROR_REJECTED:
      return std::string("rejected");
    case MSG_ERROR_STATUS_UNKNOWN:
      return std::string("status unknown");
    case MSG_ERROR_ABORTED:
      return std::string("aborted");
  }
  return std::string("unknown error");
}

int Messenger::MapPNStatusToError(pn_status_t status)
{
  switch(status) {
    case PN_STATUS_UNKNOWN:
      return MSG_ERROR_STATUS_UNKNOWN;
    case PN_STATUS_PENDING:
      return MSG_ERROR_NONE;
    case PN_STATUS_ACCEPTED: 	
      return MSG_ERROR_NONE;
    case PN_STATUS_REJECTED:
      return MSG_ERROR_REJECTED;
    case PN_STATUS_RELEASED: 	
      return MSG_ERROR_NONE;
    case PN_STATUS_MODIFIED: 	
      return MSG_ERROR_NONE;
    case PN_STATUS_ABORTED: 	
      return MSG_ERROR_ABORTED;
    case PN_STATUS_SETTLED:
      return MSG_ERROR_NONE;
  }
  return MSG_ERROR_INTERNAL;
}
