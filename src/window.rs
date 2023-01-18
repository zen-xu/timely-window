use std::collections::HashMap;
use std::vec;

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::{FrontierNotificator, Operator};
use timely::dataflow::{Scope, Stream};
use timely::progress::{PathSummary, Timestamp};
use timely::Data;

pub trait WindowBuffer<T: Timestamp, D: Data> {
    /// store data with timestamp in buffer
    fn store(&mut self, time: T, data: Vec<D>);
}

pub trait Window<G: Scope, D: Data> {
    type Buffer: WindowBuffer<G::Timestamp, D>;

    /// Get buffer reference
    fn buffer(&mut self) -> &mut Self::Buffer;

    /// Provides one record with a timestamp.
    #[inline]
    fn give(&mut self, time: G::Timestamp, datum: D) {
        self.give_vec(time, vec![datum])
    }

    /// Provides an iterator of records with a timestamp.
    #[inline]
    fn give_iterator<I: Iterator<Item = D>>(&mut self, time: G::Timestamp, iter: I) {
        self.give_vec(time, iter.collect())
    }

    /// Provides an vector of records with a timestamp.
    #[inline]
    fn give_vec(&mut self, time: G::Timestamp, data: Vec<D>) {
        self.on_new_data(&time, &data);
        self.buffer().store(time, data);
    }

    fn on_new_data(&mut self, _time: &G::Timestamp, _data: &Vec<D>) {}

    fn try_emit(&mut self) -> Option<(G::Timestamp, Vec<(G::Timestamp, D)>)>;
    fn drain(&mut self) -> Option<(G::Timestamp, Vec<(G::Timestamp, D)>)>;
}

impl<T: Timestamp, D: Data> WindowBuffer<T, D> for HashMap<T, Vec<D>> {
    fn store(&mut self, time: T, data: Vec<D>) {
        self.entry(time).or_default().extend(data);
    }
}

pub trait WindowOp<G: Scope, D: Data> {
    fn window<W: Window<G, D> + 'static>(
        &self,
        name: &str,
        window: W,
        drain_when_finished: bool,
    ) -> Stream<G, Vec<(G::Timestamp, D)>>;
}

impl<G: Scope, D: Data> WindowOp<G, D> for Stream<G, D> {
    fn window<W: Window<G, D> + 'static>(
        &self,
        name: &str,
        mut window: W,
        drain_when_finished: bool,
    ) -> Stream<G, Vec<(G::Timestamp, D)>> {
        let mut stash = HashMap::new();

        self.unary_frontier(Pipeline, &name, |cap, _| {
            let mut cap = Some(cap);
            let mut notificator = FrontierNotificator::new();

            move |input, output| {
                if input.frontier().is_empty() {
                    if drain_when_finished {
                        if let Some((emit_time, emit_data)) = window.drain() {
                            let cap = cap.as_mut().unwrap();
                            let new_time = cap.delayed(&emit_time);
                            stash
                                .entry(new_time.clone())
                                .or_insert_with(|| {
                                    notificator.notify_at(new_time);
                                    vec![]
                                })
                                .extend(emit_data)
                        }
                    }
                    cap = None;
                } else {
                    input.for_each(|time, data| {
                        window.give_vec(time.time().clone(), data.take());
                    });

                    if let Some((emit_time, emit_data)) = window.try_emit() {
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

pub struct TumblingWindow<T: Timestamp, D: Data> {
    size: T::Summary,
    emit_time: Option<T>,
    buffer: HashMap<T, Vec<D>>,
    max_time: Option<T>,
}

impl<T: Timestamp, D: Data> TumblingWindow<T, D> {
    pub fn new(size: T::Summary, init_time: Option<T>) -> Self {
        let emit_time = init_time.map(|t| size.results_in(&t).unwrap());
        Self {
            size,
            emit_time,
            buffer: Default::default(),
            max_time: None,
        }
    }
}

impl<G: Scope, D: Data> Window<G, D> for TumblingWindow<G::Timestamp, D> {
    type Buffer = HashMap<G::Timestamp, Vec<D>>;

    fn try_emit(&mut self) -> Option<(G::Timestamp, Vec<(G::Timestamp, D)>)> {
        let emit_time = self.emit_time.clone()?;
        let max_time = self.max_time.clone().unwrap();
        if max_time.lt(&emit_time) {
            return None;
        }
        let mut ready_times = self
            .buffer
            .keys()
            .filter(|time| (*time).lt(&emit_time))
            .map(Clone::clone)
            .collect::<Vec<_>>();
        ready_times.sort();

        let mut data = vec![];
        for time in ready_times {
            data.extend(
                self.buffer
                    .remove(&time)
                    .unwrap()
                    .into_iter()
                    .map(|v| (time.clone(), v))
                    .collect::<Vec<_>>(),
            );
        }

        self.emit_time = Some(self.size.results_in(&emit_time).unwrap());
        Some((emit_time.clone(), data))
    }

    fn drain(&mut self) -> Option<(<G>::Timestamp, Vec<(<G>::Timestamp, D)>)> {
        let emit_time = self.emit_time.clone()?;
        let mut reserved = vec![];
        for (time, data) in self.buffer.drain() {
            reserved.extend(
                data.into_iter()
                    .map(|v| (time.clone(), v))
                    .collect::<Vec<_>>(),
            );
        }
        if reserved.is_empty() {
            return None;
        }
        reserved.sort_by(|(t1, _), (t2, _)| t1.cmp(&t2));
        Some((emit_time, reserved))
    }

    fn buffer(&mut self) -> &mut Self::Buffer {
        &mut self.buffer
    }

    fn on_new_data(&mut self, time: &<G>::Timestamp, _data: &Vec<D>) {
        if self.emit_time.is_none() {
            self.emit_time = Some(self.size.results_in(time).unwrap());
        }

        match self.max_time.as_ref() {
            Some(max_time) if max_time.lt(time) => self.max_time = Some(time.clone()),
            None => self.max_time = Some(time.clone()),
            _ => (),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use timely::dataflow::operators::*;

    #[test]
    fn run() {
        timely::execute_directly(|worker| {
            let (mut input, probe) = worker.dataflow(|scope| {
                let (input, stream) = scope.new_input();

                let probe = stream.probe();
                stream
                    .inspect_time(|t, v| println!("A {t:?} {v:?}"))
                    .window("A", TumblingWindow::new(4, None), false)
                    .inspect_time(|t, d| println!("B {t:?} {d:?}"));
                (input, probe)
            });

            for round in 1..10_u64 {
                input.send(round);
                input.advance_to(round + 1);
                worker.step_while(|| probe.less_than(input.time()))
            }
        });
    }
}
