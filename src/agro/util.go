package agro;

import (
	"encoding/json"
	proto "github.com/golang/protobuf/proto"
	proto_json "github.com/golang/protobuf/jsonpb"
)

func ProtoToMap(pb proto.Message, idFix bool) map[string]interface{} {
	marshaler := proto_json.Marshaler{}
	s, _ := marshaler.MarshalToString(pb)
	out := make(map[string]interface{})
	json.Unmarshal([]byte(s), &out)
	if idFix {
		if _, ok := out["id"]; ok {
			out["_id"] = out["id"]
			delete(out, "id")
		}
	}
	return out
}

func MapToProto(in map[string]interface{}, pb proto.Message, idFix bool) {
	if idFix {
		if _, ok := in["_id"]; ok {
			in["id"] = in["_id"]
			delete(in, "_id")
		}
	}
	s, _ := json.Marshal(in)
	proto_json.UnmarshalString(string(s), pb)
}

/*
func ProtoToMongo(pb proto.Message, idFix bool) map[string]interface{} {
  ref := reflect.ValueOf(pb)
  out := make(map[string]interface{})
  s := ref.Elem()
  for i := 0; i < s.NumField(); i++ {
    value := s.Field(i)
    valueField := s.Type().Field(i)
    if !strings.HasPrefix(valueField.Name, "XXX_") && !strings.HasPrefix(valueField.Name, "xxx_") {
      if value.Kind() == reflect.Struct {
        out[valueField.Name] = ProtoToMongo(value.Addr().Interface().(proto.Message), false)
      } else {
        if idFix && valueField.Name == "ID" {
          out["_id"] = value.Interface()
        } else {
          out[valueField.Name] = value.Interface()
        }
      }
    }
  }
  return out
}

func MongoToProto(in interface{}, pb proto.Message, idFix bool) {
  elm := reflect.ValueOf(pb).Elem()
  elmType := elm.Type()
  for i := 0; i < elm.NumField(); i++ {
    f := elm.Field(i)
    ft := elmType.Field(i)
    if strings.HasPrefix(ft.Name, "XXX_") || strings.HasPrefix(ft.Name, "xxx_") {
      continue
    }
    name := ft.Name
    if idFix && name == "ID"{
      name = "_id"
    }
    if inVal, ok := in.(map[string]interface{})[name]; ok {
      if f.Kind() == reflect.Struct {
        MongoToProto(inVal, f.Addr().Interface().(proto.Message), false)
      } else {
        mongoToValue(inVal, f )
        //f.Set( reflect.ValueOf(inVal) )
      }
    }
  }
}

func mongoToValue(in interface{}, val reflect.Value) {
  if val.Type().Kind() == reflect.Ptr && in != nil {
    val.Set(reflect.New(val.Type().Elem()))
		mongoToValue(in, val.Elem())
    return
  }
  if val.Type().Kind() == reflect.Ptr && in == nil {
    return
  }
  if val.Type().Kind() == reflect.String {
    val.SetString(in.(string))
    return
  }
  if val.Type().Kind() == reflect.Struct && in != nil{
    MongoToProto(in, val.Addr().Interface().(proto.Message), false)
    return
  }
  if val.Type().Kind() == reflect.Slice {
    if (in != nil) {
      slc := in.([]interface{})
      len := len(slc)
      val.Set(reflect.MakeSlice(val.Type(), len, len))
  		for i := 0; i < len; i++ {
  			mongoToValue(slc[i], val.Index(i))
      }
    }
    return
  }
  if val.Type().Kind() == reflect.Uint64 || val.Type().Kind() == reflect.Int64 {
    val.SetInt( toInt64(in) )
    return
  }
  if val.Type().Kind() == reflect.Uint32 || val.Type().Kind() == reflect.Int32 {
    val.SetInt( toInt64(in) )
    return
  }
  if val.Type().Kind() == reflect.Interface {
    log.Printf("%s", val.Type())
    log.Printf("%s", val.Type().Kind())
    log.Printf("%s", proto.GetProperties(val.Type()))
  }
  log.Printf("Unknown proto type %s", val.Type().Kind())
}

func toInt64(in interface{}) int64 {
  switch (reflect.ValueOf(in).Type().Kind()) {
  case reflect.Int:
    return int64( in.(int) )
  case reflect.Int32:
    return int64( in.(int32) )
  case reflect.Uint32:
    return int64( in.(uint32) )
  case reflect.Int64:
    return int64( in.(int64) )
  case reflect.Uint64:
    return int64( in.(uint64) )
  case reflect.Float32:
    return int64( in.(float32) )
  case reflect.Float64:
    return int64( in.(float64) )
  }
  fmt.Println("Oh noes:", in, reflect.ValueOf(in).Type().Kind())
  return 0
}
*/
