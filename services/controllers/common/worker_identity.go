package common

type WorkerId string

type StreamClassWorkerIdentity interface {
	WorkerId() WorkerId
}
