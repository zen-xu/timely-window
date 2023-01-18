use std::collections::HashMap;
use std::vec;

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Operator;
use timely::dataflow::{Scope, Stream};
use timely::progress::{PathSummary, Timestamp};
use timely::Data;

pub trait Window<G: Scope, D: Data> {
    fn give(&mut self, time: G::Timestamp, data: Vec<D>);
    fn try_emit(&mut self) -> Option<(G::Timestamp, Vec<(G::Timestamp, D)>)>;
}

pub trait WindowOp<G: Scope, D: Data> {
    fn window<W: Window<G, D> + 'static>(
        &self,
        name: &str,
        window: W,
    ) -> Stream<G, Vec<(G::Timestamp, D)>>;
}

impl<G: Scope, D: Data> WindowOp<G, D> for Stream<G, D> {
    fn window<W: Window<G, D> + 'static>(
        &self,
        name: &str,
        mut window: W,
    ) -> Stream<G, Vec<(G::Timestamp, D)>> {
        let mut stash = HashMap::new();
        self.unary_notify(
            Pipeline,
            &name,
            vec![],
            move |input, output, notificator| {
                input.for_each(|time, data| {
                    let data = data.take();
                    window.give(time.time().clone(), data);
                    if let Some((emit_time, emit_data)) = window.try_emit() {
                        let new_time = time.delayed(&emit_time);
                        stash
                            .entry(time.clone())
                            .or_insert_with(|| {
                                notificator.notify_at(new_time);
                                vec![]
                            })
                            .extend(emit_data)
                    }
                });

                notificator.for_each(|time, _, _| {
                    if let Some(data) = stash.remove(&time) {
                        output.session(&time).give(data);
                    }
                });

                stash.retain(|_, v| !v.is_empty());
            },
        )
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
    fn give(&mut self, time: <G>::Timestamp, data: Vec<D>) {
        if self.emit_time.is_none() {
            self.emit_time = Some(self.size.results_in(&time).unwrap());
        }
        self.buffer.entry(time.clone()).or_default().extend(data);
        if let Some(ref max_time) = self.max_time {
            if max_time.lt(&time) {
                self.max_time = Some(time)
            }
        } else {
            self.max_time = Some(time)
        }
    }

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
                    .window("A", TumblingWindow::new(4, None))
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
