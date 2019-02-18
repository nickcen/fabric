package concorddb

import (
"context"
"log"
"time"
"google.golang.org/grpc"
"github.com/hyperledger/fabric/core/ledger/util/concorddb/msgs"
)

//ConcordInstance represents a ConcordDB instance
type ConcordInstance struct {
  address   string //connection configuration
}

//CouchDatabase represents a database within a CouchDB instance
type ConcordDatabase struct {
  instance    *ConcordInstance
  dbName      string
}

func CreateConcordInstance(address string) (*ConcordInstance) {
  // conn, err := grpc.Dial(address, grpc.WithInsecure())
  // if err != nil {
  //   log.Fatalf("did not connect: %v", err)
  // }
  // // defer conn.Close()
  // c := msgs.NewConcordClient(conn)  

  //Create the ConcordInstance instance
  concordInstance := &ConcordInstance{address: address}

  return concordInstance
}

//DropDatabase provides method to drop an existing database
func (instance *ConcordInstance) DropDatabase() (bool, error) {
  conn, err := grpc.Dial(instance.address, grpc.WithInsecure())
  if err != nil {
    log.Fatalf("did not connect: %v", err)
  }
  defer conn.Close()

  c := msgs.NewConcordClient(conn)  

  ctx, cancel := context.WithTimeout(context.Background(), time.Second)
  defer cancel()

  r, err := c.Init(ctx, &msgs.InitRequest{})
  _ = r
  if err != nil {
    return false, err
  }
  return true, nil
}


func (instance *ConcordInstance) Get(key string) ([]byte, error) {
  conn, err := grpc.Dial(instance.address, grpc.WithInsecure())
  if err != nil {
    log.Fatalf("did not connect: %v", err)
  }
  defer conn.Close()

  c := msgs.NewConcordClient(conn)  

  ctx, cancel := context.WithTimeout(context.Background(), time.Second)
  defer cancel()

  g_r, err := c.Get(ctx, &msgs.GetRequest{Key: key})
  if err != nil {
    return nil, err
  }
  return g_r.Value, nil
}

func (instance *ConcordInstance) GetIterator(startkey string, endkey string) ([]byte, error) {
  return instance.Get(startkey)
}

func (instance *ConcordInstance) Set(key string, value []byte) (bool, error) {
  conn, err := grpc.Dial(instance.address, grpc.WithInsecure())
  if err != nil {
    log.Fatalf("did not connect: %v", err)
  }
  defer conn.Close()

  c := msgs.NewConcordClient(conn)  

  ctx, cancel := context.WithTimeout(context.Background(), time.Second)
  defer cancel()

  r, err := c.Set(ctx, &msgs.SetRequest{Key: key, Value: value})

  return r.Ret, err
}

func (instance *ConcordInstance) Delete(key string) (bool, error) {
  conn, err := grpc.Dial(instance.address, grpc.WithInsecure())
  if err != nil {
    log.Fatalf("did not connect: %v", err)
  }
  defer conn.Close()

  c := msgs.NewConcordClient(conn)  

  ctx, cancel := context.WithTimeout(context.Background(), time.Second)
  defer cancel()

  r, err := c.Delete(ctx, &msgs.DeleteRequest{Key: key})
  return r.Ret, err
}

// func main() {
//   // Set up a connection to the server.
//   conn, err := grpc.Dial(address, grpc.WithInsecure())
//   if err != nil {
//     log.Fatalf("did not connect: %v", err)
//   }
//   defer conn.Close()
//   c := pb.NewConcordClient(conn)

//   ctx, cancel := context.WithTimeout(context.Background(), time.Second)
//   defer cancel()

//   s_r, err := c.Set(ctx, &pb.SetRequest{Key: "test", Value: []byte("hello world")})
//   if err != nil {
//     log.Fatalf("could not set: %v", err)
//   }
//   _ = s_r

//   g_r, err := c.Get(ctx, &pb.GetRequest{Key: "test"})
//   if err != nil {
//     log.Fatalf("could not get: %v", err)
//   }
//   log.Printf("Greeting: %s", g_r.Value)
// }
