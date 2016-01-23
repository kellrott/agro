package main

import (
    "log"
    "flag"
    "agro/engine/local"
    "google.golang.org/grpc"
    "agro/proto"
    "path/filepath"
    uuid "github.com/nu7hatch/gouuid"
    "agro/engine"
    "time"
)


func main() {
    agro_server := flag.String("agro", "localhost:9713", "Agro Server")
    workdir_arg := flag.String("workdir", "/tmp/agro_work", "Agro Workdir")
    timeout_arg := flag.Int("timeout", -1, "Timeout in seconds")

    nworker := flag.Int("nworkers", 4, "Worker Count")
    flag.Parse()
    work_dir, _ := filepath.Abs(*workdir_arg)
    log.Println("Connecting Agro Server")
    conn, err := grpc.Dial(*agro_server, grpc.WithInsecure())
    if err != nil {
      panic(err)
    }
    defer conn.Close()
    sched_client := agro_pb.NewSchedulerClient(conn)
    file_client := agro_pb.NewFileStoreClient(conn)

    u, _ := uuid.NewV4()
    manager, _ := agro_local.NewLocalManager(*nworker, work_dir, u.String())
    if *timeout_arg <= 0 {
        manager.Run(sched_client, file_client)
    } else {
        var start_count int32 = 0
        last_ping := time.Now().Unix()
        manager.SetStatusCheck( func(status agro_engine.EngineStatus) {
            if status.JobCount > start_count || status.ActiveJobs > 0 {
                start_count = status.JobCount
                last_ping = time.Now().Unix()
            }
        } )
        manager.Start(sched_client, file_client)
        for time.Now().Unix() - last_ping < int64(*timeout_arg) {
            time.Sleep(5 * time.Second)
        }

    }
}