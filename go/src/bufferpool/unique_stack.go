package bufferpool

import "sync"

// / UniqueStack is a Priority Queue / Stack that has a Uniqueness property.
// / Internally this is implemented by ~ 2*n elements
// / Reprioritizations (by "Push existing element") are slow (they involve, effectively, a delete).
// / Top, Bottom, Pop are all reasonably fast.
// / Access to the Stack is gated by a RWMutex.
type UniqueStack[K comparable] struct {
	// This is a pretty bad implementation internally.
	// It should be a tree without dupes. I hate writing rotations for tree algorithms...

	// TODO: make Data, Order private.
	Data  map[K]bool
	Order []K
	// TODO: move the mutex to a variable to be inaccessible and
	// the API fully thread-safe
	m sync.RWMutex
}

func NewUniqueStack[K comparable]() *UniqueStack[K] {
	return &UniqueStack[K]{
		Data:  map[K]bool{},
		Order: []K{},
	}

}

func (o *UniqueStack[K]) Length() int {
	o.m.RLock()
	defer o.m.RUnlock()
	return len(o.Order)
}

func (o *UniqueStack[K]) Push(e K) {
	o.m.Lock()
	defer o.m.Unlock()
	if _, ok := o.Data[e]; !ok {
		// Don't have. Push onto the top
		o.Order = append(o.Order, e)
		o.Data[e] = true
	} else {
		// Do have. Delete from current location, move to top.
		// Precondition: the element is in o.Order
		idx := 0
		for i := 0; i < len(o.Order); i++ {
			if o.Order[i] == e {
				idx = i
				break
			}
		}

		o.Order = append(o.Order[:idx], o.Order[idx+1:]...)
		o.Order = append(o.Order, e)
	}
}

func (o *UniqueStack[K]) Delete(e K) {
	// TODO
	panic("omg!")
}

func (o *UniqueStack[K]) Pop() K {
	o.m.Lock()
	defer o.m.Unlock()
	sz := len(o.Order)
	end := o.Order[sz-1]
	delete(o.Data, end)
	o.Order = o.Order[:sz-1]
	return end
}

func (o *UniqueStack[K]) Top() K {
	o.m.RLock()
	defer o.m.RUnlock()
	return o.Order[len(o.Order)-1]
}

func (o *UniqueStack[K]) Bottom() K {
	o.m.RLock()
	defer o.m.RUnlock()
	return o.Order[0]
}
