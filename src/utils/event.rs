use may::sync::RwLock;

trait FnOps<T>: Send + Sync {
    fn call_box(self: &Self, data: &T) -> ();
}

impl<T, F> FnOps<T> for F
where
    F: Fn(&T) -> () + Send + Sync,
{
    fn call_box(self: &Self, data: &T) -> () {
        (*self)(data)
    }
}

/// event handlers for a given `Event` type
pub struct EventHandlers<T: Event> {
    ops: RwLock<Vec<Box<FnOps<T>>>>,
}

impl<T: Event> Default for EventHandlers<T> {
    fn default() -> Self {
        EventHandlers {
            ops: RwLock::new(Vec::new()),
        }
    }
}

impl<T: Event + Send> EventHandlers<T> {
    fn add_op<F>(&self, f: F)
    where
        F: Fn(&T) -> () + Send + Sync + 'static,
    {
        self.ops.write().unwrap().push(Box::new(f));
    }

    fn run(&'static self, data: T) {
        let g = self.ops.read().unwrap();
        if !g.is_empty() {
            go!(move || for op in g.iter() {
                op.call_box(&data);
            });
        }
    }
}

/// Event trait
pub trait Event: Sized + Send + 'static {
    fn get_event_handlers() -> &'static EventHandlers<Self>;

    /// trigger an event, if any hanlders for the event type was registered
    /// the event handlers would be executed asynchronously
    fn trigger(self) {
        Self::get_event_handlers().run(self);
    }

    /// globally register an event handler for the event
    /// you can add any number of event handlers,
    /// each handler take a ref of the event data as parameter
    fn add_handler<F>(f: F)
    where
        F: Fn(&Self) -> () + Send + Sync + 'static,
    {
        Self::get_event_handlers().add_op(f);
    }
}

/// macro used to implement `Event` trait for a type
/// any tpyes that impl `Send` and `Sync` can be an event type
#[macro_export]
#[doc(hidden)]
macro_rules! impl_event {
    ($T:ty) => {
        impl $crate::utils::event::Event for $T {
            fn get_event_handlers() -> &'static $crate::utils::event::EventHandlers<Self> {
                lazy_static! {
                    static ref HANDLERS: $crate::utils::event::EventHandlers<$T> =
                        $crate::utils::event::EventHandlers::default();
                }
                &*HANDLERS
            }
        }
    };
}

/// emit an event, if any hanlders for the event type was registered
/// the event handlers would be executed asynchronously
pub fn emit_event<T: Event>(event: T) {
    event.trigger();
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_event() {
        struct MyEvent {
            data: u32,
        }
        impl_event!(MyEvent);
        let s = MyEvent { data: 42 };
        MyEvent::add_handler(|s| assert_eq!(s.data % 2, 0));
        MyEvent::add_handler(|s| assert_eq!(s.data, 42));
        s.trigger();
    }

    #[test]
    fn test_emit_event() {
        impl_event!(u32);
        <u32 as Event>::add_handler(|v| assert_eq!(*v, 64));
        emit_event(64);
    }
}
