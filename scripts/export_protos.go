// Maintenance tool: run from the reference Go module to capture its exact API.
// Normal C++ builds use the checked-in descriptor set and need no Go installation.
package main
import (
 "os"
 "sort"
 pb "github.com/envoyproxy/go-control-plane/envoy/service/ratelimit/v3"
 ds "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
 conf "github.com/envoyproxy/go-control-plane/ratelimit/config/ratelimit/v3"
 "google.golang.org/protobuf/proto"
 "google.golang.org/protobuf/reflect/protoreflect"
 "google.golang.org/protobuf/reflect/protodesc"
 "google.golang.org/protobuf/types/descriptorpb"
)
func main() {
 files := map[string]protoreflect.FileDescriptor{}
 var visit func(protoreflect.FileDescriptor)
 visit = func(f protoreflect.FileDescriptor) { if _,ok:=files[f.Path()];ok{return}; files[f.Path()]=f; for i:=0;i<f.Imports().Len();i++ {visit(f.Imports().Get(i).FileDescriptor)} }
 visit((&pb.RateLimitRequest{}).ProtoReflect().Descriptor().ParentFile())
 visit((&ds.DiscoveryRequest{}).ProtoReflect().Descriptor().ParentFile())
 visit((&conf.RateLimitConfig{}).ProtoReflect().Descriptor().ParentFile())
 names:=[]string{}; for name:=range files {names=append(names,name)};sort.Strings(names)
 set:=&descriptorpb.FileDescriptorSet{};for _,name:=range names {set.File=append(set.File,protodesc.ToFileDescriptorProto(files[name]))}
 data,err:=proto.Marshal(set);if err!=nil{panic(err)};if err=os.WriteFile(os.Args[1],data,0644);err!=nil{panic(err)}
}
