use timely::progress::PathSummary;
use timely::progress::Timestamp;

use crate::{EmitResult, Watermark, Window, WindowBuffer};

pub struct TumblingWindow<B: WindowBuffer> {
    size: <B::Timestamp as Timestamp>::Summary,
    emit_time: Option<B::Timestamp>,
    buffer: B,
}

impl<B: WindowBuffer> TumblingWindow<B> {
    pub fn new(
        size: <B::Timestamp as Timestamp>::Summary,
        init_time: Option<B::Timestamp>,
    ) -> Self {
        let emit_time = init_time.map(|t| size.results_in(&t).unwrap());
        Self {
            size,
            emit_time,
            buffer: B::default(),
        }
    }
}

impl<B: WindowBuffer> Window<B> for TumblingWindow<B> {
    fn buffer(&mut self) -> &mut B {
        &mut self.buffer
    }

    fn on_new_data(&mut self, time: &B::Timestamp, _data: &[B::Datum]) {
        if self.emit_time.is_none() {
            self.emit_time = Some(self.size.results_in(time).unwrap());
        }
    }

    fn try_emit(&mut self, watermark: Watermark<B::Timestamp>) -> EmitResult<B> {
        let emit_time = self.emit_time.take()?;

        if watermark.less_equal(&emit_time) {
            self.emit_time = Some(emit_time);
            return None;
        }

        let mut ready_times = self
            .buffer
            .timestamps()
            .into_iter()
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
    use std::collections::HashMap;
    use timely::dataflow::operators::*;

    #[test]
    fn run() {
        timely::execute_directly(|worker| {
            let (mut input, probe) = worker.dataflow(|scope| {
                let (input, stream) = scope.new_input();

                let probe = stream.probe();
                stream
                    .inspect_time(|t, v| println!("A {t:?} {v:?}"))
                    .window("A", TumblingWindow::<HashMap<_, _>>::new(4, None))
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
