#ifndef _PROTONDATA_H_
#define _PROTONDATA_H_

#include <node.h>

#include "proton/types.h"
#include "proton/codec.h"

using namespace v8;
using namespace node;

class ProtonData {
  private:
    static pn_type_t JSTypeToPNType(std::string type);
    static std::string PNTypeToJSType(pn_type_t type);
    static bool IsSimpleValue(pn_type_t type);
    static Handle<Value> GetSimpleValue(pn_data_t *data);
    static Handle<Value> GetDescribedValue(pn_data_t *data);
    static Handle<Value> GetArrayValue(pn_data_t *data);
    static Handle<Value> GetListValue(pn_data_t *data);
    static Handle<Value> GetMapValue(pn_data_t *data);
    static Handle<Value> ParseValue(pn_data_t *data);
    static pn_data_t *GetSimpleJSValue(pn_type_t type, Local<Value> jsval);
    static pn_data_t *GetDescribedJSValue(Handle<Array> array);
    static pn_data_t *GetArrayOrListJSValue(pn_type_t type, Handle<Array> array);
    static pn_data_t *GetArrayJSValue(Handle<Array> array);
    static pn_data_t *GetListJSValue(Handle<Array> array);
    static pn_data_t *GetMapJSValue(Handle<Array> array);

  public:
    static Local<Array> ParsePnData(pn_data_t *data);
    static pn_data_t *ParseJSData(Handle<Value> jsval);  
};

#endif /* _PROTONDATA_H_ */