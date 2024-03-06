/*
 * Copyright 2023, Sayan Nandan <nandansayan@outlook.com>
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

use crate::{
    config::Config,
    error::{ClientResult, ConnectionSetupError, Error},
    response::{Response, Row, Value},
};

pub(crate) type ProtocolResult<T> = Result<T, ProtocolError>;

/// Errors that can happen when handling protocol level encoding and decoding
#[derive(Debug, PartialEq, Clone)]
pub enum ProtocolError {
    /// The server returned an invalid response for the data item
    InvalidServerResponseForData,
    /// The server possibly returned an unknown data type and we can't decode it. Note that this might happen when you use an older client version with
    /// a newer version of Skytable
    InvalidServerResponseUnknownDataType,
    InvalidPacket,
}

impl Value {
    fn u64(self) -> u64 {
        match self {
            Self::UInt64(u) => u,
            _ => unreachable!(),
        }
    }
}

/*
    Decode state management
*/

type ValueDecodeStateRaw = ValueDecodeStateAny<ValueState>;
type ValueDecodeState = ValueDecodeStateAny<PendingValue>;

#[derive(Debug, Default, PartialEq)]
struct MetaState {
    completed: bool,
    val: u64,
}

impl MetaState {
    fn new(completed: bool, val: u64) -> Self {
        Self { completed, val }
    }
    #[inline(always)]
    fn finished(&mut self, decoder: &mut Decoder) -> ProtocolResult<bool> {
        self.finish_or_continue(decoder, || Ok(true), || Ok(false), |e| Err(e))
    }
    #[inline(always)]
    fn finish_or_continue<T>(
        &mut self,
        decoder: &mut Decoder,
        if_completed: impl FnOnce() -> T,
        if_pending: impl FnOnce() -> T,
        if_err: impl FnOnce(ProtocolError) -> T,
    ) -> T {
        Self::try_finish_or_continue(
            self.completed,
            &mut self.val,
            decoder,
            if_completed,
            if_pending,
            if_err,
        )
    }
    #[inline(always)]
    fn try_finish(decoder: &mut Decoder, completed: bool, val: &mut u64) -> ProtocolResult<bool> {
        Self::try_finish_or_continue(
            completed,
            val,
            decoder,
            || Ok(true),
            || Ok(false),
            |e| Err(e),
        )
    }
    #[inline(always)]
    fn try_finish_or_continue<T>(
        completed: bool,
        val: &mut u64,
        decoder: &mut Decoder,
        if_completed: impl FnOnce() -> T,
        if_pending: impl FnOnce() -> T,
        if_err: impl FnOnce(ProtocolError) -> T,
    ) -> T {
        if completed {
            if_completed()
        } else {
            match decoder.__resume_decode(*val, ValueStateMeta::zero()) {
                Ok(vs) => match vs {
                    ValueDecodeStateAny::Pending(ValueState { v, .. }) => {
                        *val = v.u64();
                        if_pending()
                    }
                    ValueDecodeStateAny::Decoded(v) => {
                        *val = v.u64();
                        if_completed()
                    }
                },
                Err(e) => if_err(e),
            }
        }
    }
    #[inline(always)]
    fn val(&self) -> u64 {
        self.val
    }
}

#[derive(Debug, PartialEq)]
enum ValueDecodeStateAny<P, V = Value> {
    Pending(P),
    Decoded(V),
}

#[derive(Debug, PartialEq)]
struct ValueState {
    v: Value,
    meta: ValueStateMeta,
}

impl ValueState {
    fn new(v: Value, meta: ValueStateMeta) -> Self {
        Self { v, meta }
    }
}

#[derive(Debug, PartialEq)]
struct ValueStateMeta {
    start: usize,
    md: MetaState,
}

impl ValueStateMeta {
    fn zero() -> Self {
        Self {
            start: 0,
            md: MetaState::default(),
        }
    }
    fn new(start: usize, md1: u64, md1_flag: bool) -> Self {
        Self {
            start,
            md: MetaState::new(md1_flag, md1),
        }
    }
}

#[derive(Debug, PartialEq)]
struct RowState {
    meta: ValueStateMeta,
    row: Vec<Value>,
    tmp: Option<PendingValue>,
}

impl RowState {
    fn new(meta: ValueStateMeta, row: Vec<Value>, tmp: Option<PendingValue>) -> Self {
        Self { meta, row, tmp }
    }
}

#[derive(Debug, PartialEq)]
struct MultiRowState {
    c_row: Option<RowState>,
    rows: Vec<Row>,
    md_state: u8,
    md1_target: u64,
    md2_col_cnt: u64,
}

impl Default for MultiRowState {
    fn default() -> Self {
        Self::new(None, vec![], 0, 0, 0)
    }
}

impl MultiRowState {
    fn new(c_row: Option<RowState>, rows: Vec<Row>, md_s: u8, md_cnt: u64, md_target: u64) -> Self {
        Self {
            c_row,
            rows,
            md_state: md_s,
            md1_target: md_target,
            md2_col_cnt: md_cnt,
        }
    }
}

#[derive(Debug, PartialEq)]
enum ResponseState {
    Initial,
    PValue(PendingValue),
    PError,
    PRow(RowState),
    PMultiRow(MultiRowState),
}

#[derive(Debug, PartialEq)]
pub enum DecodeState {
    ChangeState(RState),
    Completed(Response),
    Error(ProtocolError),
}

#[derive(Debug, PartialEq)]
pub struct RState(ResponseState);
impl Default for RState {
    fn default() -> Self {
        RState(ResponseState::Initial)
    }
}

/*
    Decoder
*/

#[derive(Debug, PartialEq)]
pub struct Decoder<'a> {
    b: &'a [u8],
    i: usize,
}

impl<'a> Decoder<'a> {
    pub const MIN_READBACK: usize = 1;
    pub fn new(b: &'a [u8], i: usize) -> Self {
        Self { b, i }
    }
    pub fn validate_response(&mut self, RState(state): RState) -> DecodeState {
        match state {
            ResponseState::Initial => self.begin(),
            ResponseState::PError => self.resume_error(),
            ResponseState::PValue(v) => self.resume_value(v),
            ResponseState::PRow(r) => self.resume_row(r),
            ResponseState::PMultiRow(mr) => self.resume_rows(mr),
        }
    }
    pub fn position(&self) -> usize {
        self.i
    }
    fn begin(&mut self) -> DecodeState {
        match self._cursor_next() {
            // TODO(@ohsayan): this is reserved!
            0x0F => return DecodeState::Error(ProtocolError::InvalidServerResponseUnknownDataType),
            0x10 => self.resume_error(),
            0x11 => self.resume_row(RowState::new(ValueStateMeta::zero(), vec![], None)),
            0x12 => return DecodeState::Completed(Response::Empty),
            0x13 => self.resume_rows(MultiRowState::default()),
            code => match self.start_decode(true, code, vec![], None) {
                Ok(ValueDecodeStateAny::Decoded(v)) => DecodeState::Completed(Response::Value(v)),
                Ok(ValueDecodeStateAny::Pending(pv)) => {
                    DecodeState::ChangeState(RState(ResponseState::PValue(pv)))
                }
                Err(e) => DecodeState::Error(e),
            },
        }
    }
    fn resume_error(&mut self) -> DecodeState {
        if self._remaining() < 2 {
            return DecodeState::ChangeState(RState(ResponseState::PError));
        }
        let bytes: [u8; 2] = [self._cursor_next(), self._cursor_next()];
        DecodeState::Completed(Response::Error(u16::from_le_bytes(bytes)))
    }
    fn resume_value(&mut self, PendingValue { state, tmp, stack }: PendingValue) -> DecodeState {
        match self.resume_decode(true, state, stack, tmp) {
            Ok(ValueDecodeStateAny::Pending(pv)) => {
                DecodeState::ChangeState(RState(ResponseState::PValue(pv)))
            }
            Ok(ValueDecodeStateAny::Decoded(v)) => DecodeState::Completed(Response::Value(v)),
            Err(e) => DecodeState::Error(e),
        }
    }
    fn resume_row(&mut self, mut row_state: RowState) -> DecodeState {
        match row_state.meta.md.finished(self) {
            Ok(true) => self._decode_row_core(row_state),
            Ok(false) => DecodeState::ChangeState(RState(ResponseState::PRow(row_state))),
            Err(e) => DecodeState::Error(e),
        }
    }
    fn _decode_row_core(&mut self, mut row_state: RowState) -> DecodeState {
        while row_state.row.len() as u64 != row_state.meta.md.val() {
            let r = match row_state.tmp.take() {
                None => {
                    if self._cursor_eof() {
                        return DecodeState::ChangeState(RState(ResponseState::PRow(row_state)));
                    }
                    let code = self._cursor_next();
                    let stack = vec![];
                    self.start_decode(true, code, stack, None)
                }
                Some(PendingValue { state, tmp, stack }) => {
                    self.resume_decode(true, state, stack, tmp)
                }
            };
            let r = match r {
                Ok(r) => r,
                Err(e) => return DecodeState::Error(e),
            };
            match r {
                ValueDecodeStateAny::Pending(pv) => {
                    row_state.tmp = Some(pv);
                    return DecodeState::ChangeState(RState(ResponseState::PRow(row_state)));
                }
                ValueDecodeStateAny::Decoded(v) => {
                    row_state.row.push(v);
                }
            }
        }
        DecodeState::Completed(Response::Row(Row::new(row_state.row)))
    }
    fn resume_rows(&mut self, mut multirow: MultiRowState) -> DecodeState {
        macro_rules! finish {
            ($completed:expr, $target:expr) => {
                match MetaState::try_finish(self, $completed, &mut $target) {
                    Ok(true) => multirow.md_state += 1,
                    Ok(false) => {
                        return DecodeState::ChangeState(RState(ResponseState::PMultiRow(multirow)))
                    }
                    Err(e) => return DecodeState::Error(e),
                }
            };
        }
        finish!(multirow.md_state == 1, &mut multirow.md1_target);
        finish!(multirow.md_state == 2, &mut multirow.md2_col_cnt);
        while multirow.rows.len() as u64 != multirow.md1_target {
            let ret = match multirow.c_row.take() {
                Some(r) => self._decode_row_core(r),
                None => self._decode_row_core(RowState::new(
                    ValueStateMeta::new(0, multirow.md2_col_cnt, true),
                    vec![],
                    None,
                )),
            };
            match ret {
                DecodeState::Completed(Response::Row(r)) => multirow.rows.push(r),
                DecodeState::Completed(_) => unreachable!(),
                e @ DecodeState::Error(_) => return e,
                DecodeState::ChangeState(RState(ResponseState::PRow(pr))) => {
                    multirow.c_row = Some(pr);
                    return DecodeState::ChangeState(RState(ResponseState::PMultiRow(multirow)));
                }
                DecodeState::ChangeState(_) => unreachable!(),
            }
        }
        DecodeState::Completed(Response::Rows(multirow.rows))
    }
}

impl<'a> Decoder<'a> {
    fn __resume_decode<T: DecodeDelimited>(
        &mut self,
        mut value: T,
        meta: ValueStateMeta,
    ) -> ProtocolResult<ValueDecodeStateRaw> {
        let mut okay = true;
        while !(self._cursor_eof() | self._creq(b'\n')) & okay {
            okay &= value.update(self._cursor_next());
        }
        let lf = self._creq(b'\n');
        self._cursor_incr_if(lf);
        okay &= !(lf & (self._cursor() == meta.start));
        if okay & lf {
            let start = meta.start;
            value
                .pack_completed(meta, &self.b[start..self._cursor() - 1])
                .map(ValueDecodeStateRaw::Decoded)
        } else {
            if okay {
                Ok(ValueDecodeStateAny::Pending(value.pack_pending(meta)))
            } else {
                Err(ProtocolError::InvalidServerResponseForData)
            }
        }
    }
    fn __resume_psize<T: DecodePsize>(
        &mut self,
        mut meta: ValueStateMeta,
    ) -> ProtocolResult<ValueDecodeStateRaw> {
        if !meta.md.finished(self)? {
            Ok(ValueDecodeStateRaw::Pending(ValueState::new(
                T::empty(),
                meta,
            )))
        } else {
            meta.start = self._cursor();
            if self._remaining() as u64 >= meta.md.val() {
                let buf = &self.b[meta.start..self._cursor() + meta.md.val() as usize];
                self._cursor_incr_by(meta.md.val() as usize);
                T::finish(buf).map(ValueDecodeStateAny::Decoded)
            } else {
                Ok(ValueDecodeStateAny::Pending(ValueState::new(
                    T::empty(),
                    meta,
                )))
            }
        }
    }
}

impl<'a> Decoder<'a> {
    fn _cursor(&self) -> usize {
        self.i
    }
    fn _cursor_value(&self) -> u8 {
        self.b[self._cursor()]
    }
    fn _cursor_incr(&mut self) {
        self._cursor_incr_by(1)
    }
    fn _cursor_incr_by(&mut self, b: usize) {
        self.i += b;
    }
    fn _cursor_incr_if(&mut self, iff: bool) {
        self._cursor_incr_by(iff as _)
    }
    fn _cursor_next(&mut self) -> u8 {
        let r = self._cursor_value();
        self._cursor_incr();
        r
    }
    fn _remaining(&self) -> usize {
        self.b.len() - self.i
    }
    fn _cursor_eof(&self) -> bool {
        self._remaining() == 0
    }
    fn _creq(&self, b: u8) -> bool {
        (self.b[core::cmp::min(self.i, self.b.len() - 1)] == b) & !self._cursor_eof()
    }
}

trait DecodeDelimited {
    fn update(&mut self, _: u8) -> bool {
        true
    }
    fn pack_completed(self, meta: ValueStateMeta, full_buffer: &[u8]) -> ProtocolResult<Value>;
    fn pack_pending(self, meta: ValueStateMeta) -> ValueState;
}

trait DecodePsize {
    fn finish(b: &[u8]) -> ProtocolResult<Value>;
    fn empty() -> Value;
}

impl DecodePsize for Vec<u8> {
    fn finish(b: &[u8]) -> ProtocolResult<Value> {
        Ok(Value::Binary(b.to_owned()))
    }
    fn empty() -> Value {
        Value::Binary(vec![])
    }
}

impl DecodePsize for String {
    fn finish(b: &[u8]) -> ProtocolResult<Value> {
        core::str::from_utf8(b)
            .map(String::from)
            .map(Value::String)
            .map_err(|_| ProtocolError::InvalidServerResponseForData)
    }
    fn empty() -> Value {
        Value::String(String::new())
    }
}

macro_rules! impl_uint {
    ($($ty:ty as $variant:ident),*) => {
        $(impl DecodeDelimited for $ty {
            fn update(&mut self, b: u8) -> bool {
                let mut okay = true; let (r1, of_1) = self.overflowing_mul(10);
                okay &= !of_1; let (r2, of_2) = r1.overflowing_add((b & 0x0f) as $ty);
                okay &= !of_2;
                okay &= b.is_ascii_digit(); *self = r2; okay
            }
            fn pack_pending(self, meta: ValueStateMeta) -> ValueState { ValueState::new(Value::$variant(self), meta) }
            fn pack_completed(self, _: ValueStateMeta, _: &[u8]) -> ProtocolResult<Value> { Ok(Value::$variant(self)) }
        })*
    }
}

macro_rules! impl_fstr {
    ($($ty:ty as $variant:ident),*) => {
        $(impl DecodeDelimited for $ty {
            fn pack_pending(self, meta: ValueStateMeta) -> ValueState { ValueState::new(Value::$variant(self), meta) }
            fn pack_completed(self, _: ValueStateMeta, b: &[u8]) -> ProtocolResult<Value> {
                core::str::from_utf8(b).map_err(|_| ProtocolError::InvalidServerResponseForData)?.parse().map(Value::$variant).map_err(|_| ProtocolError::InvalidServerResponseForData)
            }
        })*
    };
}

impl_uint!(u8 as UInt8, u16 as UInt16, u32 as UInt32, u64 as UInt64);
impl_fstr!(
    i8 as SInt8,
    i16 as SInt16,
    i32 as SInt32,
    i64 as SInt64,
    f32 as Float32,
    f64 as Float64
);

#[derive(Debug, PartialEq)]
struct PendingValue {
    state: ValueState,
    tmp: Option<ValueState>,
    stack: Vec<(Vec<Value>, ValueStateMeta)>,
}

impl PendingValue {
    fn new(
        state: ValueState,
        tmp: Option<ValueState>,
        stack: Vec<(Vec<Value>, ValueStateMeta)>,
    ) -> Self {
        Self { state, tmp, stack }
    }
}

impl<'a> Decoder<'a> {
    fn parse_list(
        &mut self,
        mut stack: Vec<(Vec<Value>, ValueStateMeta)>,
        mut last: Option<ValueState>,
    ) -> ProtocolResult<ValueDecodeStateAny<PendingValue, Value>> {
        let (mut current_list, mut current_meta) = stack.pop().unwrap();
        loop {
            if !current_meta.md.finished(self)? {
                return Ok(ValueDecodeStateAny::Pending(PendingValue::new(
                    ValueState::new(Value::List(vec![]), ValueStateMeta::zero()),
                    None,
                    stack,
                )));
            }
            if current_list.len() as u64 == current_meta.md.val() {
                match stack.pop() {
                    None => {
                        return Ok(ValueDecodeStateAny::Decoded(Value::List(current_list)));
                    }
                    Some((mut parent, parent_meta)) => {
                        parent.push(Value::List(current_list));
                        current_list = parent;
                        current_meta = parent_meta;
                        continue;
                    }
                }
            }
            let v = match last.take() {
                None => {
                    // nothing present, we need to decode
                    if self._cursor_eof() {
                        // wow, nothing here
                        stack.push((current_list, current_meta));
                        return Ok(ValueDecodeStateAny::Pending(PendingValue::new(
                            ValueState::new(Value::List(vec![]), ValueStateMeta::zero()),
                            None,
                            stack,
                        )));
                    }
                    match self._cursor_next() {
                        0x0E => {
                            // that's a list
                            stack.push((current_list, current_meta));
                            current_list = vec![];
                            current_meta = ValueStateMeta::zero();
                            continue;
                        }
                        code => self.start_decode(false, code, vec![], None),
                    }
                }
                Some(v) => self.resume_decode(false, v, vec![], None),
            }?;
            let v = match v {
                ValueDecodeStateAny::Pending(pv) => {
                    stack.push((current_list, current_meta));
                    return Ok(ValueDecodeStateAny::Pending(PendingValue::new(
                        ValueState::new(Value::List(vec![]), ValueStateMeta::zero()),
                        Some(pv.state),
                        stack,
                    )));
                }
                ValueDecodeStateAny::Decoded(v) => v,
            };
            current_list.push(v);
        }
    }
}

impl<'a> Decoder<'a> {
    fn start_decode(
        &mut self,
        root: bool,
        code: u8,
        mut stack: Vec<(Vec<Value>, ValueStateMeta)>,
        last: Option<ValueState>,
    ) -> ProtocolResult<ValueDecodeState> {
        let md = ValueStateMeta::new(self._cursor(), 0, false);
        let v = match code {
            0x00 => return Ok(ValueDecodeStateAny::Decoded(Value::Null)),
            0x01 => return self.parse_bool(stack),
            0x02 => self.__resume_decode(0u8, md),
            0x03 => self.__resume_decode(0u16, md),
            0x04 => self.__resume_decode(0u32, md),
            0x05 => self.__resume_decode(0u64, md),
            0x06 => self.__resume_decode(0i8, md),
            0x07 => self.__resume_decode(0i16, md),
            0x08 => self.__resume_decode(0i32, md),
            0x09 => self.__resume_decode(0i64, md),
            0x0A => self.__resume_decode(0f32, md),
            0x0B => self.__resume_decode(0f64, md),
            0x0C => self.__resume_psize::<Vec<u8>>(md),
            0x0D => self.__resume_psize::<String>(md),
            0x0E => {
                if !root {
                    unreachable!("recursive structure not captured by root");
                }
                stack.push((vec![], ValueStateMeta::zero()));
                return self.parse_list(stack, last);
            }
            _ => return Err(ProtocolError::InvalidServerResponseUnknownDataType),
        }?;
        Self::check_pending(v, stack)
    }
    fn resume_decode(
        &mut self,
        root: bool,
        ValueState { v, meta }: ValueState,
        stack: Vec<(Vec<Value>, ValueStateMeta)>,
        last: Option<ValueState>,
    ) -> ProtocolResult<ValueDecodeState> {
        let r = match v {
            Value::Null => unreachable!(),
            Value::Bool(_) => return self.parse_bool(stack),
            Value::UInt8(l) => self.__resume_decode(l, meta),
            Value::UInt16(l) => self.__resume_decode(l, meta),
            Value::UInt32(l) => self.__resume_decode(l, meta),
            Value::UInt64(l) => self.__resume_decode(l, meta),
            Value::SInt8(l) => self.__resume_decode(l, meta),
            Value::SInt16(l) => self.__resume_decode(l, meta),
            Value::SInt32(l) => self.__resume_decode(l, meta),
            Value::SInt64(l) => self.__resume_decode(l, meta),
            Value::Float32(l) => self.__resume_decode(l, meta),
            Value::Float64(l) => self.__resume_decode(l, meta),
            Value::Binary(_) => self.__resume_psize::<Vec<u8>>(meta),
            Value::String(_) => self.__resume_psize::<String>(meta),
            Value::List(_) => {
                if !root {
                    unreachable!("recursive structure not captured by root");
                }
                return self.parse_list(stack, last);
            }
        }?;
        Self::check_pending(r, stack)
    }
    fn parse_bool(
        &mut self,
        stack: Vec<(Vec<Value>, ValueStateMeta)>,
    ) -> ProtocolResult<ValueDecodeState> {
        if self._cursor_eof() {
            return Ok(ValueDecodeStateAny::Pending(PendingValue::new(
                ValueState::new(Value::Bool(false), ValueStateMeta::zero()),
                None,
                stack,
            )));
        }
        let nx = self._cursor_next();
        if nx < 2 {
            return Ok(ValueDecodeStateAny::Decoded(Value::Bool(nx == 1)));
        } else {
            return Err(ProtocolError::InvalidServerResponseForData);
        }
    }
    fn check_pending(
        r: ValueDecodeStateAny<ValueState, Value>,
        stack: Vec<(Vec<Value>, ValueStateMeta)>,
    ) -> Result<ValueDecodeStateAny<PendingValue, Value>, ProtocolError> {
        match r {
            ValueDecodeStateAny::Pending(p) => Ok(ValueDecodeStateAny::Pending(PendingValue::new(
                p, None, stack,
            ))),
            ValueDecodeStateAny::Decoded(v) => Ok(ValueDecodeStateAny::Decoded(v)),
        }
    }
}

pub struct ClientHandshake(Box<[u8]>);
impl ClientHandshake {
    pub(crate) fn new(cfg: &Config) -> Self {
        let mut v = Vec::with_capacity(6 + cfg.username().len() + cfg.password().len() + 5);
        v.extend(b"H\x00\x00\x00\x00\x00");
        pushlen!(v, cfg.username().len());
        pushlen!(v, cfg.password().len());
        v.extend(cfg.username().as_bytes());
        v.extend(cfg.password().as_bytes());
        Self(v.into_boxed_slice())
    }
    pub(crate) fn inner(&self) -> &[u8] {
        &self.0
    }
}

#[derive(Debug)]
pub enum ServerHandshake {
    Okay(u8),
    Error(u8),
}
impl ServerHandshake {
    pub fn parse(v: [u8; 4]) -> ClientResult<Self> {
        Ok(match v {
            [b'H', 0, 0, msg] => Self::Okay(msg),
            [b'H', 0, 1, msg] => Self::Error(msg),
            _ => {
                return Err(Error::ConnectionSetupErr(
                    ConnectionSetupError::InvalidServerHandshake,
                ))
            }
        })
    }
}

#[derive(Debug, PartialEq, Default)]
pub(crate) struct MRespState {
    processed: Vec<Response>,
    pending: Option<ResponseState>,
    expected: MetaState,
}

#[derive(Debug, PartialEq)]
pub(crate) enum PipelineResult {
    Completed(Vec<Response>),
    Pending(MRespState),
    Error(ProtocolError),
}

impl MRespState {
    fn step(mut self, decoder: &mut Decoder) -> PipelineResult {
        match self.expected.finished(decoder) {
            Ok(true) => {}
            Ok(false) => return PipelineResult::Pending(self),
            Err(e) => return PipelineResult::Error(e),
        }
        loop {
            if self.processed.len() as u64 == self.expected.val() {
                return PipelineResult::Completed(self.processed);
            }
            match decoder.validate_response(RState(
                self.pending.take().unwrap_or(ResponseState::Initial),
            )) {
                DecodeState::ChangeState(RState(s)) => {
                    self.pending = Some(s);
                    return PipelineResult::Pending(self);
                }
                DecodeState::Completed(c) => self.processed.push(c),
                DecodeState::Error(e) => return PipelineResult::Error(e),
            }
        }
    }
}

impl<'a> Decoder<'a> {
    pub fn validate_pipe(&mut self, first: bool, state: MRespState) -> PipelineResult {
        if first && self._cursor_next() != b'P' {
            PipelineResult::Error(ProtocolError::InvalidPacket)
        } else {
            state.step(self)
        }
    }
}

#[test]
fn t_row() {
    let mut decoder = Decoder::new(b"\x115\n\x00\x01\x01\x0D5\nsayan\x0220\n\x0E0\n", 0);
    assert_eq!(
        decoder.validate_response(RState::default()),
        DecodeState::Completed(Response::Row(Row::new(vec![
            Value::Null,
            Value::Bool(true),
            Value::String("sayan".into()),
            Value::UInt8(20),
            Value::List(vec![])
        ])))
    );
}

#[test]
fn t_mrow() {
    let mut decoder = Decoder::new(b"\x133\n5\n\x00\x01\x01\x0D5\nsayan\x0220\n\x0E0\n\x00\x01\x01\x0D5\nelana\x0221\n\x0E0\n\x00\x01\x01\x0D5\nemily\x0222\n\x0E0\n", 0);
    assert_eq!(
        decoder.validate_response(RState::default()),
        DecodeState::Completed(Response::Rows(vec![
            Row::new(vec![
                Value::Null,
                Value::Bool(true),
                Value::String("sayan".into()),
                Value::UInt8(20),
                Value::List(vec![])
            ]),
            Row::new(vec![
                Value::Null,
                Value::Bool(true),
                Value::String("elana".into()),
                Value::UInt8(21),
                Value::List(vec![])
            ]),
            Row::new(vec![
                Value::Null,
                Value::Bool(true),
                Value::String("emily".into()),
                Value::UInt8(22),
                Value::List(vec![])
            ])
        ]))
    );
}

#[test]
fn t_pipe() {
    let mut decoder = Decoder::new(b"P5\n\x12\x10\xFF\xFF\x115\n\x00\x01\x01\x0D5\nsayan\x0220\n\x0E0\n\x115\n\x00\x01\x01\x0D5\nelana\x0221\n\x0E0\n\x115\n\x00\x01\x01\x0D5\nemily\x0222\n\x0E0\n", 0);
    assert_eq!(
        decoder.validate_pipe(true, MRespState::default()),
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
