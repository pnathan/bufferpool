use std::collections::HashSet;
use std::hash::Hash;

pub struct UniqueStack<T> {
    order: Vec<T>,
    unique: HashSet<T>,
}

impl<T> UniqueStack<T>
where
    T: Eq + PartialEq + Hash + Clone,
{
    pub fn new() -> UniqueStack<T> {
        UniqueStack {
            order: Vec::new(),
            unique: HashSet::new(),
        }
    }

    pub fn push(&mut self, item: T) {
        if self.unique.contains(&item) {
            let idx = self.order.iter().position(|x| *x == item).unwrap();
            self.order.remove(idx);
        }
        let i = item.clone();
        self.unique.insert(i);
        self.order.push(item);
    }

    pub fn delete(&mut self, item: T) {
        if self.unique.contains(&item) {
            let idx = self.order.iter().position(|x| *x == item).unwrap();
            self.order.remove(idx);
            self.unique.remove(&item);
        }
    }

    pub fn pop(&mut self) -> Option<T> {
        let item = self.order.pop();
        if let Some(x) = item.clone() {
            self.unique.remove(&x);
        }
        item
    }

    // Returns the most recently pushed item, or None if the stack is empty.
    pub fn top(&self) -> Option<T> {
        self.order.last().map(|x| (*x).clone())
    }

    // Returns the least recently pushed item, or None if the stack is empty.
    pub fn bottom(&self) -> Option<T> {
        self.order.first().map(|x| (*x).clone())
    }

    // Returns a copy of the items, in order.
    pub fn order(&self) -> Vec<T> {
        self.order.iter().map(|x| (*x).clone()).collect()
    }

    pub fn contains(&self, item: &T) -> bool {
        self.unique.contains(item)
    }

    pub fn len(&self) -> u64 {
        self.order.len() as u64
    }

    pub fn is_empty(&self) -> bool {
        self.order.is_empty()
    }
}

impl<T> Default for UniqueStack<T>
where
    T: Eq + PartialEq + Hash + Clone,
{
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_push() {
        let mut stack = UniqueStack::new();
        stack.push(1);
        stack.push(2);
        stack.push(3);
        assert_eq!(stack.len(), 3);
        assert_eq!(stack.top(), Some(3));
        assert_eq!(stack.bottom(), Some(1));
    }

    #[test]
    fn test_pop() {
        let mut stack = UniqueStack::new();
        stack.push(1);
        stack.push(2);
        stack.push(3);
        assert_eq!(stack.pop(), Some(3));
        assert_eq!(stack.len(), 2);
        assert_eq!(stack.top(), Some(2));
        assert_eq!(stack.bottom(), Some(1));
    }

    #[test]
    fn test_contains() {
        let mut stack = UniqueStack::new();
        stack.push(1);
        stack.push(2);
        stack.push(3);
        assert!(stack.contains(&1));
        assert!(stack.contains(&2));
        assert!(stack.contains(&3));
        assert!(!stack.contains(&4));
    }

    #[test]
    fn test_push_duplicate() {
        let mut stack = UniqueStack::new();
        stack.push(1);
        stack.push(2);
        stack.push(3);
        stack.push(2);
        assert_eq!(stack.len(), 3);
        assert_eq!(stack.top(), Some(2));
        assert_eq!(stack.bottom(), Some(1));
    }

    #[test]
    fn test_pop_duplicate() {
        let mut stack = UniqueStack::new();
        stack.push(1);
        stack.push(2);
        stack.push(3);
        stack.push(2);
        assert_eq!(stack.pop(), Some(2));
        assert_eq!(stack.len(), 2);
        assert_eq!(stack.top(), Some(3));
        assert_eq!(stack.bottom(), Some(1));
    }

    #[test]
    fn test_delete() {
        let mut stack = UniqueStack::new();
        stack.push(1);
        stack.push(2);
        stack.push(3);
        stack.delete(2);
        assert_eq!(stack.len(), 2);
        assert_eq!(stack.top(), Some(3));
        assert_eq!(stack.bottom(), Some(1));
        stack.delete(3);
        assert!(!stack.contains(&3));
        assert_eq!(stack.len(), 1);
        stack.delete(1);
        assert!(!stack.contains(&1));
        assert_eq!(stack.len(), 0);
    }
}
