package consulreg

import (
	"context"
	"fmt"

	capi "github.com/hashicorp/consul/api"
)

type Registrar struct{ c *capi.Client }

func New(c *capi.Client) *Registrar { return &Registrar{c: c} }

type Service struct {
	Name     string
	ID       string
	Address  string
	Port     int
	Tags     []string
	TCPCheck bool
}

func (r *Registrar) Register(ctx context.Context, s Service) error {
	reg := &capi.AgentServiceRegistration{
		Name: s.Name, ID: s.ID, Address: s.Address, Port: s.Port, Tags: s.Tags,
	}
	if s.TCPCheck {
		reg.Check = &capi.AgentServiceCheck{
			TCP:                             fmt.Sprintf("%s:%d", s.Address, s.Port),
			Interval:                        "5s",
			Timeout:                         "1s",
			DeregisterCriticalServiceAfter:  "30s",
		}
	}
	return r.c.Agent().ServiceRegister(reg)
}

func (r *Registrar) Deregister(ctx context.Context, id string) error {
	return r.c.Agent().ServiceDeregister(id)
}
