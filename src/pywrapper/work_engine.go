
package pyagro

import (
	"agro_engine"
)


type PyEngine struct {
	engine *agro_engine.Engine
}

func Engine() PyEngine {
	return PyEngine{ engine: agro_engine.NewEngine() }
}
