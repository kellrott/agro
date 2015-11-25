package main


import (
    "log"
    "flag"
    "agro"
    "agro/db"
    "agro/engine"
    "agro/engine/local"
)


func main() {
    mongo := flag.String("mongo", "localhost", "Mongo Server")
    workdir := flag.String("workdir", "/tmp/agro_work", "Agro Workdir")
    flag.Parse()
    log.Println("Starting DB Connection")
    dbi, _ := agro_db.NewMongo(*mongo)
    var manager agro_engine.JobManager = nil
    manager, _ = agro_local.NewLocalManager(4, *workdir)
    
    engine, _ := agro_engine.NewEngine(dbi, manager, 4)
    server := agro.NewAgroServer()
    server.RegisterEngine(engine)
    server.RegisterFileStore(dbi)
    
    engine.Start()
    
    server.Run()
    
}