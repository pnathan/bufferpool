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
	Data  map[K]bool
	Order []K
	sync.RWMutex
}

func NewUniqueStack[K comparable]() *UniqueStack[K] {
	return &UniqueStack[K]{
		Data:  map[K]bool{},
		Order: []K{},
	}

}

func (o *UniqueStack[K]) Length() int {
	o.RLock()
	defer o.RUnlock()
	return len(o.Order)
}

func (o *UniqueStack[K]) Push(e K) {
	o.Lock()
	defer o.Unlock()
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

func (o *UniqueStack[K]) Pop() K {
	o.Lock()
	defer o.Unlock()
	sz := len(o.Order)
	end := o.Order[sz-1]
	delete(o.Data, end)
	o.Order = o.Order[:sz-1]
	return end
}

func (o *UniqueStack[K]) Top() K {
	o.RLock()
	defer o.RUnlock()
	return o.Order[len(o.Order)-1]
}

func (o *UniqueStack[K]) Bottom() K {
	o.RLock()
	defer o.RUnlock()
	return o.Order[0]
}
