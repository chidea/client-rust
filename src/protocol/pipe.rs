/*
 * Copyright 2024, Sayan Nandan <nandansayan@outlook.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

use {
    super::{
        state::{DecodeState, RState, ResponseState},
        Decoder, ProtocolError,
    },
    crate::response::Response,
};

const ILLEGAL_PACKET_ESCAPE: u8 = 0xFF;

#[derive(Debug, PartialEq, Default)]
pub(crate) struct MRespState {
    processed: Vec<Response>,
    pending: Option<ResponseState>,
}

#[derive(Debug, PartialEq)]
pub(crate) enum PipelineResult {
    Completed(Vec<Response>),
    Pending(MRespState),
    Error(ProtocolError),
}

impl MRespState {
    #[cold]
    fn except() -> PipelineResult {
        PipelineResult::Error(ProtocolError::InvalidPacket)
    }
    fn step(mut self, decoder: &mut Decoder, expected: usize) -> PipelineResult {
        loop {
            if decoder._cursor_eof() {
                return PipelineResult::Pending(self);
            }
            if decoder._cursor_value() == ILLEGAL_PACKET_ESCAPE {
                return Self::except();
            }
            match decoder.validate_response(RState(
                self.pending.take().unwrap_or(ResponseState::Initial),
            )) {
                DecodeState::ChangeState(RState(s)) => {
                    self.pending = Some(s);
                    return PipelineResult::Pending(self);
                }
                DecodeState::Completed(c) => {
                    self.processed.push(c);
                    if self.processed.len() == expected {
                        return PipelineResult::Completed(self.processed);
                    }
                }
                DecodeState::Error(e) => return PipelineResult::Error(e),
            }
        }
    }
}

impl<'a> Decoder<'a> {
    pub fn validate_pipe(&mut self, expected: usize, state: MRespState) -> PipelineResult {
        state.step(self, expected)
    }
}

#[cfg(test)]
const QUERY: &[u8] = b"\x12\x10\xFF\xFF\x115\n\x00\x01\x01\x0D5\nsayan\x0220\n\x0E0\n\x115\n\x00\x01\x01\x0D5\nelana\x0221\n\x0E0\n\x115\n\x00\x01\x01\x0D5\nemily\x0222\n\x0E0\n";

#[test]
fn t_pipe() {
    use crate::response::{Response, Row, Value};
    let mut decoder = Decoder::new(QUERY, 0);
    assert_eq!(
        decoder.validate_pipe(5, MRespState::default()),
        PipelineResult::Completed(vec![
            Response::Empty,
            Response::Error(u16::MAX),
            Response::Row(Row::new(vec![
                Value::Null,
                Value::Bool(true),
                Value::String("sayan".into()),
                Value::UInt8(20),
                Value::List(vec![])
            ])),
            Response::Row(Row::new(vec![
                Value::Null,
                Value::Bool(true),
                Value::String("elana".into()),
                Value::UInt8(21),
                Value::List(vec![])
            ])),
            Response::Row(Row::new(vec![
                Value::Null,
                Value::Bool(true),
                Value::String("emily".into()),
                Value::UInt8(22),
                Value::List(vec![])
            ]))
        ])
    );
}

#[test]
fn t_pipe_staged() {
    for i in Decoder::MIN_READBACK..QUERY.len() {
        let mut dec = Decoder::new(&QUERY[..i], 0);
        if i < 3 {
            assert!(matches!(
                dec.validate_pipe(5, MRespState::default()),
                PipelineResult::Pending(_)
            ));
        } else {
            assert!(matches!(
                dec.validate_pipe(5, MRespState::default()),
                PipelineResult::Pending(_)
            ));
        }
    }
}
