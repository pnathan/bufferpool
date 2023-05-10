use std::intrinsics::transmute;
use std::sync::Mutex;
use serde::{Deserialize, Serialize};

// the Innerframe contains the data for a Frame's mutex.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct InnerFrame<T>
where
    T: Clone,
{
    data: Box<T>,
    pins: u32,
    dirty: bool,
}

// A frame is a container for data to be written.
#[derive(Debug)]
pub struct PageFrame<T>
where
    T: Clone,
{
    mutex: Mutex<InnerFrame<T>>,
}

impl<T> PartialEq for PageFrame<T> where T : Clone + PartialEq {
    fn eq(&self, other: &Self) -> bool {
        let data = self.mutex.lock().unwrap();
        let o2 =other.mutex.lock().unwrap();
        *data == *o2
    }
}

impl<T> PageFrame<T>
where
    T: Clone,
{
    pub fn boxed_new(boxed: Box<T>) -> Self {
        PageFrame {
            mutex: Mutex::new( InnerFrame{
                data: boxed,
                pins: 0,
                dirty: false
            })
        }
    }

    pub fn new(data: T) -> Self {
        PageFrame {
            mutex: Mutex::new(InnerFrame {
                data: Box::new(data),
                pins: 0,
                dirty: false,
            }),
        }
    }

    pub fn pin(&self) {
        let mut inner = self.mutex.lock().unwrap();
        inner.pins += 1;
    }

    pub fn unpin(&self) {
        let mut inner = self.mutex.lock().unwrap();
        inner.pins -= 1;
    }

    pub fn is_pinned(&self) -> bool {
        let inner = self.mutex.lock().unwrap();
        inner.pins > 0
    }

    pub fn is_dirty(&self) -> bool {
        let inner = self.mutex.lock().unwrap();
        inner.dirty
    }

    pub fn set_dirty(&self, dirty: bool) {
        let mut inner = self.mutex.lock().unwrap();
        inner.dirty = dirty;
    }

    // has a clone.
    pub fn data(&self) -> T {
        let inner = self.mutex.lock().unwrap();
        // TODO: address the clone.
        *inner.data.clone()
    }

    pub fn put(&self, data: T) {
        let mut inner = self.mutex.lock().unwrap();
        *inner.data = data;
    }

    // with_data is the designated function that allows you to modify the data in the frame.
    pub fn with_data<F>(&self, f: F)
    where
        F: FnOnce(&mut T),
    {
        let mut inner = self.mutex.lock().unwrap();
        f(&mut inner.data.as_mut());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_frame_happy() {
        let v = vec![10, 20, 30];
        let pf = PageFrame::new(v);
        let mut extracted : Vec<i32> = Vec::new();
        pf.with_data(|x: &mut Vec<i32>| {
            println!("{:?}", x);

            let v = vec![-100, -200, -300];
            *x = v;
            println!("{:?}", x);

            extracted = x.clone();
        });

        let extracted = extracted;
        println!("{:?}", extracted);
    }
}