use std::collections::HashMap;

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::{FrontierNotificator, Operator};
use timely::dataflow::{Scope, Stream};
use timely::progress::frontier::MutableAntichain;
use timely::progress::Timestamp;
use timely::Data;

pub trait WindowBuffer: Default {
    type Timestamp: Timestamp;
    type Datum: Data;

    /// List stored timestamps
    fn timestamps(&self) -> Vec<&Self::Timestamp>;

    /// Store data with timestamp in buffer
    fn store(&mut self, time: Self::Timestamp, data: Vec<Self::Datum>);

    /// Remove buffered timestamp and pop its data
    fn remove(&mut self, time: &Self::Timestamp) -> Option<Vec<Self::Datum>>;
}

impl<T: Timestamp, D: Data> WindowBuffer for HashMap<T, Vec<D>> {
    type Timestamp = T;
    type Datum = D;

    fn timestamps(&self) -> Vec<&Self::Timestamp> {
        self.keys().collect::<Vec<_>>()
    }

    fn store(&mut self, time: Self::Timestamp, data: Vec<Self::Datum>) {
        self.entry(time).or_default().extend(data);
    }

    fn remove(&mut self, time: &Self::Timestamp) -> Option<Vec<Self::Datum>> {
        HashMap::<T, Vec<D>>::remove(self, time)
    }
}

pub struct Watermark<'w, T: Timestamp> {
    inner: &'w MutableAntichain<T>,
}

impl<'w, T: Timestamp> Watermark<'w, T> {
    fn new(antichain: &'w MutableAntichain<T>) -> Self {
        Self { inner: antichain }
    }

    pub fn less_than(&self, time: &T) -> bool {
        self.inner.less_than(time)
    }

    pub fn less_equal(&self, time: &T) -> bool {
        self.inner.less_equal(time)
    }
}

pub type EmitResult<B> = Option<(
    <B as WindowBuffer>::Timestamp,
    Vec<(<B as WindowBuffer>::Timestamp, <B as WindowBuffer>::Datum)>,
)>;

pub trait Window<B: WindowBuffer> {
    /// Get buffer reference
    fn buffer(&mut self) -> &mut B;

    /// Provides one record with a timestamp.
    #[inline]
    fn give(&mut self, time: B::Timestamp, datum: B::Datum) {
        self.give_vec(time, vec![datum])
    }

    /// Provides an iterator of records with a timestamp.
    #[inline]
    fn give_iterator<I: Iterator<Item = B::Datum>>(&mut self, time: B::Timestamp, iter: I) {
        self.give_vec(time, iter.collect())
    }

    /// Provides an vector of records with a timestamp.
    #[inline]
    fn give_vec(&mut self, time: B::Timestamp, data: Vec<B::Datum>) {
        self.on_new_data(&time, &data);
        self.buffer().store(time, data);
    }

    /// The hook which will be invoked when given new data
    fn on_new_data(&mut self, _time: &B::Timestamp, _data: &[B::Datum]) {}

    /// Try to emit data from buffer by the given watermark
    fn try_emit(&mut self, watermark: Watermark<B::Timestamp>) -> EmitResult<B>;
}

pub trait WindowOp<G: Scope, D: Data> {
    fn window<W: Window<B> + 'static, B: WindowBuffer<Timestamp = G::Timestamp, Datum = D>>(
        &self,
        name: &str,
        window: W,
    ) -> Stream<G, Vec<(G::Timestamp, D)>>;
}

impl<G: Scope, D: Data> WindowOp<G, D> for Stream<G, D> {
    fn window<W: Window<B> + 'static, B: WindowBuffer<Timestamp = G::Timestamp, Datum = D>>(
        &self,
        name: &str,
        mut window: W,
    ) -> Stream<G, Vec<(G::Timestamp, D)>> {
        let mut stash = HashMap::new();

        self.unary_frontier(Pipeline, name, |cap, _| {
            let mut cap = Some(cap);
            let mut notificator = FrontierNotificator::new();

            move |input, output| {
                if input.frontier().is_empty() {
                    cap = None;
                } else {
                    input.for_each(|time, data| {
                        window.give_vec(time.time().clone(), data.take());
                    });

                    if let Some((emit_time, emit_data)) =
                        window.try_emit(Watermark::new(input.frontier()))
                    {
                        let cap = cap.as_mut().unwrap();
                        let new_time = cap.delayed(&emit_time);
                        cap.downgrade(&emit_time);
                        stash
                            .entry(new_time.clone())
                            .or_insert_with(|| {
                                notificator.notify_at(new_time);
                                vec![]
                            })
                            .extend(emit_data)
                    }
                }

                notificator.for_each(&[input.frontier()], |time, _| {
                    if let Some(data) = stash.remove(&time) {
                        output.session(&time).give(data);
                    }
                });

                stash.retain(|_, v| !v.is_empty());
            }
        })
    }
}
