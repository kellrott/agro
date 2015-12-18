package main


import (
    "log"
    "flag"
    "agro"
    "agro/db"
)


func main() {
    mongo := flag.String("mongo", "localhost", "Mongo Server")
    flag.Parse()
    log.Println("Starting DB Connection")
    dbi, _ := agro_db.NewMongo(*mongo)

    server := agro.NewAgroServer()
    server.RegisterScheduler(dbi)
    server.RegisterFileStore(dbi)
    server.Run()
    
}