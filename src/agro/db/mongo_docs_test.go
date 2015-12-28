package agro_db


import (
	"agro/proto"
	"testing"
	"github.com/gogo/protobuf/proto"
	"github.com/mesos/mesos-go/examples/Godeps/_workspace/src/golang.org/x/net/context"
	"fmt"
	"log"
)



func TestDocument(t *testing.T) {
	doc := agro_pb.Document{
		Id: proto.String("test"),
		Fields: []*agro_pb.Field{
			&agro_pb.Field{Path:[]string{"hello"}, Value:&agro_pb.Field_FloatValue{FloatValue:35.0}},
		},
	}

	dbi, err := NewMongo("localhost")
	if err != nil {
		t.Error("DB Failure")
		panic(err)
	}
	t.Log("Testing")
	fmt.Print("Testing2")
	dbi.CreateDoc(context.Background(), &doc)

	out, _ := dbi.GetDoc(context.Background(), &agro_pb.FileID{Id:proto.String("test")})

	log.Print("output: ", out)

}




func TestPack_1(t *testing.T) {
	doc_before := map[string]interface{} {
		"Hello" : "world",
		"map_data" : map[string]interface{} {
			"data" : "map",
		},
		"array_data" : []interface{} {
			"data",
			"array",
		},
		"mapped_arrays" : map[string]interface{} {
			"mapped_array_data_1" : []interface{} {
				"data",
				"array",
			},
			"mapped_array_data_2" : []interface{} {
				"data",
				"array",
			},
		},
	}

	for i := range StreamFields(doc_before) {
		log.Printf("%#v", i)
	}

	d := PackDoc("test", doc_before)
	doc_after := UnpackDoc(d)

	log.Printf("%#v", doc_before)
	log.Printf("%#v", doc_after)

}




func TestPack_2(t *testing.T) {
	doc_before := map[string]interface{} {
		"mapped_array_data_1" : []interface{} {
			"data",
			"array",
		},
		"mapped_array_data_2" : []interface{} {
			"data",
			"array",
		},
	}

	d := PackDoc("test", doc_before)
	doc_after := UnpackDoc(d)

	log.Printf("%#v", doc_before)
	log.Printf("%#v", doc_after)

}


func TestPack_3_1(t *testing.T) {
	doc_before := map[string]interface{} {
		"1" : []interface{} {
			map[string]interface{} {
				"item" : 1,
			},
		},
	}

	d := PackDoc("test", doc_before)
	doc_after := UnpackDoc(d)
	for _, f := range d.Fields {
		log.Printf("Field %#v", f)
	}

	log.Printf("%#v", doc_before)
	log.Printf("%#v", doc_after)

}



func TestPack_3(t *testing.T) {
	doc_before := map[string]interface{} {
		"mapped_array_data_1" : []interface{} {
			map[string]interface{} {
				"item" : 1,
				"value" : true,
			},
			map[string]interface{} {
				"item" : 2,
				"value" : false,
			},
		},
	}

	d := PackDoc("test", doc_before)
	doc_after := UnpackDoc(d)
	for _, f := range d.Fields {
		log.Printf("Field %#v", f)
	}

	log.Printf("%#v", doc_before)
	log.Printf("%#v", doc_after)

}


