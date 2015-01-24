#include <string>
#include <vector>
#include <map>

#include "protondata.h"

pn_type_t ProtonData::JSTypeToPNType(std::string type)
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

std::string ProtonData::PNTypeToJSType(pn_type_t type)
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

bool ProtonData::IsSimpleValue(pn_type_t type)
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

Handle<Value> ProtonData::GetSimpleValue(pn_data_t *data)
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

Handle<Value> ProtonData::GetDescribedValue(pn_data_t *data)
{
  HandleScope scope;
  
  pn_data_enter(data);
  Handle<Value> description = ProtonData::ParsePnData(data);
  pn_data_next(data);
  Handle<Value> value = ProtonData::ParsePnData(data);
  pn_data_exit(data);
  
  Handle<Object> t(Object::New());
  t->Set(String::New("description"), description);
  t->Set(String::New("value"), value);
  
  return t;
}

Handle<Value> ProtonData::GetArrayValue(pn_data_t *data)
{
  HandleScope scope;
  
  Handle<Array> t = Array::New();
  int idx = 0;
  pn_data_enter(data);
  while(pn_data_next(data)) {
    t->Set(idx++, ProtonData::ParsePnData(data));
  }
  
  return t;
}

Handle<Value> ProtonData::GetListValue(pn_data_t *data)
{
  return ProtonData::GetArrayValue(data);
}

Handle<Value> ProtonData::GetMapValue(pn_data_t *data)
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

Handle<Value> ProtonData::ParseValue(pn_data_t *data)
{
  if(ProtonData::IsSimpleValue(pn_data_type(data))) {
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

Local<Array> ProtonData::ParsePnData(pn_data_t *data)
{
  HandleScope scope;
  Local<Array> t = Array::New();
  pn_data_next(data);
  t->Set(0, String::New(PNTypeToJSType(pn_data_type(data)).c_str()));
  t->Set(1, ParseValue(data));
  return scope.Close(t);
}

pn_data_t *ProtonData::GetSimpleJSValue(pn_type_t type, Local<Value> jsval)
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

pn_data_t *ProtonData::GetDescribedJSValue(Handle<Array> array)
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

pn_data_t *ProtonData::GetArrayOrListJSValue(pn_type_t type, Handle<Array> array)
{
  pn_data_t *rval = pn_data(0);
  pn_type_t arraytype = PN_NULL;
  int length = array->Length() - 1;
  
  if(type == PN_LIST) {
    pn_data_put_list(rval);
  } else {
    if(length > 0) {
      pn_data_t *data = ParseJSData(array->Get(1));
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

pn_data_t *ProtonData::GetArrayJSValue(Handle<Array> array)
{
  return GetArrayOrListJSValue(PN_ARRAY, array);
}

pn_data_t *ProtonData::GetListJSValue(Handle<Array> array)
{
  return GetArrayOrListJSValue(PN_LIST, array);
}

pn_data_t *ProtonData::GetMapJSValue(Handle<Array> array)
{
  pn_data_t *rval = NULL;
  int length = array->Length() - 1;
  
  if((length % 2) == 0) {
    rval = pn_data(0);
    pn_data_put_map(rval);
    pn_data_enter(rval);
    unsigned int nodecnt = length / 2;
    for(unsigned int i = 0; i < nodecnt; i++) {
      pn_data_t *key = ParseJSData(array->Get(i * 2 + 1));
      if(key) {
        pn_data_t *value = ParseJSData(array->Get((i + 1) * 2));
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

pn_data_t *ProtonData::ParseJSData(Handle<Value> jsval)
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
      Handle<Array> value = Handle<Array>::Cast(jsval);
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
  } else {
    Local<Value> value = Local<Value>::New(jsval);
    if(jsval->IsString()) {
      rval = GetSimpleJSValue(PN_STRING, value);
    } else if(jsval->IsBoolean()) {
      rval = GetSimpleJSValue(PN_BOOL, value);
    } else if(jsval->IsNull()) {
      rval = GetSimpleJSValue(PN_NULL, value);
    } else {
      // TODO -- BINARY, others?
    }
  }

  return rval;
}