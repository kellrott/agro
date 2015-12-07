package main


import (
    "log"
    "flag"
    "agro/db"
    "agro/engine"
    "agro/engine/mesos"
    "agro"
)


func main() {
    mongo := flag.String("mongo", "localhost", "Mongo Server")
    mesos_master := flag.String("mesos", "localhost:5050", "Mesos Server")

    flag.Parse()
    log.Println("Starting DB Connection")
    dbi, _ := agro_db.NewMongo(*mongo)
    var manager agro_engine.JobManager = nil
    manager, _ = agro_mesos.NewMesosManager(*mesos_master)
    
    engine, _ := agro_engine.NewEngine(dbi, manager, 4)
    
    server := agro.NewAgroServer()
    server.RegisterEngine(engine)
    server.RegisterFileStore(dbi)    
    engine.Start()
    server.Run()    
}