use std::collections::HashMap;
use std::vec;

use timely::progress::Timestamp;
use timely::Data;
use timely::{dataflow::Scope, progress::PathSummary};

use crate::generic::{Watermark, Window};

pub struct TumblingWindow<T: Timestamp, D: Data> {
    size: T::Summary,
    emit_time: Option<T>,
    buffer: HashMap<T, Vec<D>>,
}

impl<T: Timestamp, D: Data> TumblingWindow<T, D> {
    pub fn new(size: T::Summary, init_time: Option<T>) -> Self {
        let emit_time = init_time.map(|t| size.results_in(&t).unwrap());
        Self {
            size,
            emit_time,
            buffer: Default::default(),
        }
    }
}

impl<G: Scope, D: Data> Window<G, D> for TumblingWindow<G::Timestamp, D> {
    type Buffer = HashMap<G::Timestamp, Vec<D>>;

    fn buffer(&mut self) -> &mut Self::Buffer {
        &mut self.buffer
    }

    fn on_new_data(&mut self, time: &<G>::Timestamp, _data: &Vec<D>) {
        if self.emit_time.is_none() {
            self.emit_time = Some(self.size.results_in(time).unwrap());
        }
    }

    fn try_emit<'w>(
        &mut self,
        watermark: Watermark<'w, G::Timestamp>,
    ) -> Option<(<G>::Timestamp, Vec<(<G>::Timestamp, D)>)> {
        let emit_time = self.emit_time.take()?;

        if watermark.less_equal(&emit_time) {
            self.emit_time = Some(emit_time);
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

        // update next emit time
        self.emit_time = Some(self.size.results_in(&emit_time).unwrap());
        Some((emit_time, data))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generic::WindowOp;
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
