package discovery

import (
	"log"
	"sync"

	"github.com/go-zookeeper/zk"
	"github.com/yext/curator"
)

type TreeCache struct {
	*ServiceDiscovery

	existingMu sync.Mutex
	existing   map[string]map[string]*ServiceInstance

	serviceListChanges  chan bool
	instanceListChanges chan string
}

func NewTreeCache(s *ServiceDiscovery) *TreeCache {
	existing := make(map[string]map[string]*ServiceInstance)
	return &TreeCache{
		ServiceDiscovery:    s,
		existing:            existing,
		serviceListChanges:  make(chan bool, 10),
		instanceListChanges: make(chan string, 10),
	}
}

func (t *TreeCache) Start() {
	//t.processServiceChanges()
	go t.processInstanceChanges()
}

func (t *TreeCache) processInstanceChanges() {
	for {
		s, ok := getMostRecentString(t.instanceListChanges)
		if !ok {
			break
		}
		t.readAndWatch(s, "restarting")
	}
	log.Println("Done watching for instance changes")
}

func (t *TreeCache) WatchService(service string) {
	t.servicesMu.Lock()
	_, ok := t.Services[service]
	t.servicesMu.Unlock()
	if !ok {
		t.readAndWatch(service, "watch")
	}
}

func (t *TreeCache) readAndWatch(service, verb string) {
	p := t.pathForName(service)
	w := curator.NewWatcher(func(e *zk.Event) { t.instanceListChanges <- service })
	children, err := t.client.GetChildren().UsingWatcher(w).ForPath(p)

	if err != nil {
		log.Printf("Error %s watch for %s: %s\n", verb, service, err)
	} else {
		t.readInstanceList(service, children)
	}
}

func (t *TreeCache) readInstanceList(s string, children []string) {
	t.existingMu.Lock()
	defer t.existingMu.Unlock()

	instances := make([]*ServiceInstance, 0, len(children))

	existing, ok := t.existing[s]
	if !ok {
		existing = make(map[string]*ServiceInstance)
	}

	for _, id := range children {
		if e, found := existing[id]; found {
			instances = append(instances, e)
			continue
		}

		p := t.pathForInstance(s, id)
		data, err := t.client.GetData().ForPath(p)
		if err != nil {
			log.Printf("Error fetching instance info for %s-%s (%s): %s\n", s, id, p, err)
			continue
		}

		i, err := t.serializer.Deserialize(data)
		if err != nil {
			log.Printf("Error decoding instance info for %s-%s (%s): %s\n", s, id, p, err)
			continue
		}
		i.Id = id // Just in case, since we treat use path for caching.

		existing[id] = i
		instances = append(instances, i)
	}
	t.existing[s] = existing

	t.servicesMu.Lock()
	t.Services[s] = instances
	t.servicesMu.Unlock()
}

func (t *TreeCache) processServiceChanges() {
	watching := make(map[string]bool)

	go func() {
		for {
			_, cont := getMostRecentBool(t.serviceListChanges)
			if !cont {
				break
			}

			t.readServices(watching)
		}
		log.Println("Done watching for service changes")
	}()
	t.readServices(watching)
}

func (t *TreeCache) readServices(watching map[string]bool) {
	w := curator.NewWatcher(func(*zk.Event) { t.serviceListChanges <- true })

	children, err := t.client.GetChildren().UsingWatcher(w).ForPath(t.basePath)
	if err != nil {
		log.Println("Error reading service list: ", err)
		return
	}
	found := make(map[string]bool)
	for _, i := range children {
		found[i] = true
		if !watching[i] {
			t.readAndWatch(i, "starting")
			watching[i] = true
		}
	}

	t.servicesMu.Lock()
	for i, _ := range t.Services {
		if !found[i] {
			delete(t.Services, i)
		}
	}
	t.servicesMu.Unlock()
}

func (s *TreeCache) Provider(name string) ServiceProvider {
	return s.ProviderWithStrategy(name, NewRandomProvider())
}

func (s *TreeCache) ProviderWithStrategy(name string, strat ProviderStrategy) ServiceProvider {
	s.WatchService(name)
	return &ServiceDiscoveryInstanceProvider{name, s.ServiceDiscovery, strat}
}
