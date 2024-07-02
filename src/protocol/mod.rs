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

pub mod handshake;
mod pipe;

use crate::response::Row;

use {
    crate::response::{Response, Value},
    std::marker::PhantomData,
};
// re-export
pub(crate) use pipe::{MRespState, PipelineResult};

/// A [`Result`] type for results originating from the protocol module
pub type ProtocolResult<T> = Result<T, ProtocolError>;

/// Errors that can happen when handling protocol level encoding and decoding
#[derive(Debug, PartialEq, Clone)]
pub enum ProtocolError {
    /// The server returned an invalid response for the data item
    InvalidServerResponseForData,
    /// The server possibly returned an unknown data type and we can't decode it. Note that this might happen when you use an older client version with
    /// a newer version of Skytable
    InvalidServerResponseUnknownDataType,
    /// The server responded with an unknown packet structure (are you correctly pairing database and database client versions?)
    InvalidPacket,
}

#[derive(Debug, PartialEq)]
pub enum DecodeState {
    ChangeState(RState),
    Completed(Response),
    Error(ProtocolError),
}

#[derive(Debug, PartialEq)]
pub struct RState(pub(super) ResponseState);
impl Default for RState {
    fn default() -> Self {
        RState(ResponseState::Initial)
    }
}

#[derive(Debug, PartialEq)]
pub(crate) enum ResponseState {
    Initial,
    PValue(PendingValue),
    PError,
    PRow(ValueStream),
    PMultiRow(MultiValueStream),
}

/*
    decoder
*/

#[derive(Debug)]
/// Skyhash/2 decoder
pub struct Decoder<'a> {
    b: &'a [u8],
    i: usize,
}

impl<'a> Decoder<'a> {
    #[cfg(test)]
    /// the minimum number of bytes
    pub const MIN_READBACK: usize = 1;
    /// Initialize the decoder
    pub fn new(b: &'a [u8], i: usize) -> Self {
        Self { b, i }
    }
    /// get the current position of the decoder
    pub fn position(&self) -> usize {
        self.i
    }
    pub fn validate_response(mut self, RState(state): RState) -> (DecodeState, usize) {
        let ret = match state {
            ResponseState::Initial => {
                match self.next() {
                    // TODO(@ohsayan): this is reserved!
                    0x0F => DecodeState::Error(ProtocolError::InvalidServerResponseUnknownDataType),
                    0x10 => self.complete_error(),
                    0x11 => self.complete_row(ValueStream::initialize(&self)),
                    0x12 => DecodeState::Completed(Response::Empty),
                    0x13 => self.complete_rows(MultiValueStream::initialize(&self)),
                    code => match PendingValue::next_value_with_code(&mut self, code) {
                        Ok(ds) => match ds {
                            ProtocolObjectDecodeState::Completed(c) => {
                                DecodeState::Completed(Response::Value(c))
                            }
                            ProtocolObjectDecodeState::Pending(pv) => {
                                DecodeState::ChangeState(RState(ResponseState::PValue(pv)))
                            }
                        },
                        Err(e) => DecodeState::Error(e),
                    },
                }
            }
            ResponseState::PValue(pv) => match pv.try_complete_self(&mut self) {
                Ok(ds) => match ds {
                    ProtocolObjectDecodeState::Completed(c) => {
                        DecodeState::Completed(Response::Value(c))
                    }
                    ProtocolObjectDecodeState::Pending(pv) => {
                        DecodeState::ChangeState(RState(ResponseState::PValue(pv)))
                    }
                },
                Err(e) => DecodeState::Error(e),
            },
            ResponseState::PError => self.complete_error(),
            ResponseState::PRow(vs) => self.complete_row(vs),
            ResponseState::PMultiRow(mvs) => self.complete_rows(mvs),
        };
        (ret, self.position())
    }
    fn complete_error(&mut self) -> DecodeState {
        if self.remaining() < 2 {
            DecodeState::ChangeState(RState(ResponseState::PError))
        } else {
            let bytes: [u8; 2] = [self.next(), self.next()];
            DecodeState::Completed(Response::Error(u16::from_le_bytes(bytes)))
        }
    }
    fn complete_row(&mut self, value_stream: ValueStream) -> DecodeState {
        match value_stream.complete(self) {
            Ok(ds) => match ds {
                ProtocolObjectDecodeState::Completed(valuestream) => {
                    DecodeState::Completed(Response::Row(Row::new(valuestream.items)))
                }
                ProtocolObjectDecodeState::Pending(prow) => {
                    DecodeState::ChangeState(RState(ResponseState::PRow(prow)))
                }
            },
            Err(e) => DecodeState::Error(e),
        }
    }
    fn complete_rows(&mut self, mvs: MultiValueStream) -> DecodeState {
        match mvs.complete(self) {
            Ok(ds) => match ds {
                ProtocolObjectDecodeState::Completed(c) => {
                    DecodeState::Completed(Response::Rows(unsafe { core::mem::transmute(c.items) }))
                }
                ProtocolObjectDecodeState::Pending(pmv) => {
                    DecodeState::ChangeState(RState(ResponseState::PMultiRow(pmv)))
                }
            },
            Err(e) => DecodeState::Error(e),
        }
    }
}

impl<'a> Decoder<'a> {
    fn next(&mut self) -> u8 {
        let r = self.b[self.i];
        self.i += 1;
        r
    }
    fn remaining(&self) -> usize {
        self.current().len()
    }
    fn current(&self) -> &[u8] {
        &self.b[self.i..]
    }
    fn eof(&self) -> bool {
        self.current().is_empty()
    }
    fn cursor_value(&self) -> u8 {
        self.current()[0]
    }
    fn cursor_eq(&self, b: u8) -> bool {
        (self.b[self.i.min(self.b.len() - 1)] == b) && !self.eof()
    }
    fn has_left(&self, s: usize) -> bool {
        self.remaining() >= s
    }
    fn next_chunk(&mut self, size: usize) -> &[u8] {
        let current = self.i;
        let chunk = &self.b[current..current + size];
        self.i += size;
        chunk
    }
}

/*
    common state mgmt
*/

trait ProtocolObjectState: Sized {
    type Value;
    fn initialize(decoder: &Decoder) -> Self;
    fn complete(self, decoder: &mut Decoder) -> ProtocolResult<ProtocolObjectDecodeState<Self>>;
    fn into_value(self) -> Self::Value;
}

#[derive(Debug, PartialEq)]
enum ProtocolObjectDecodeState<T, U = T> {
    Completed(T),
    Pending(U),
}

impl<T, U: ProtocolObjectState<Value = T>> ProtocolObjectDecodeState<T, U> {
    fn try_complete(
        self,
        decoder: &mut Decoder,
    ) -> ProtocolResult<ProtocolObjectDecodeState<T, U>> {
        match self {
            Self::Completed(c) => Ok(Self::Completed(c)),
            Self::Pending(pv) => match pv.complete(decoder)? {
                ProtocolObjectDecodeState::Completed(c) => Ok(Self::Completed(c.into_value())),
                ProtocolObjectDecodeState::Pending(pv) => Ok(Self::Pending(pv)),
            },
        }
    }
}

#[cfg(test)]
impl<T: ProtocolObjectState + core::fmt::Debug> ProtocolObjectDecodeState<T> {
    fn into_completed(self) -> Option<T> {
        match self {
            Self::Completed(c) => Some(c),
            Self::Pending(_) => None,
        }
    }
}

/*
    protocol objects:
    1. lfsobject -> lf separated object
    2. spobject -> size prefixed object
*/

pub(crate) trait LfsObject: Sized {
    type State;
    fn init_state(decoder: &Decoder) -> (Self, Self::State);
    /// return false if the byte can't be accepted
    fn update(&mut self, state: &mut Self::State, byte: u8) -> bool;
    /// the byte stream has reached EOF. parse this object
    fn complete_lfs(self, _: &Self::State, _: &Decoder) -> ProtocolResult<Self> {
        Ok(self)
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct LfsValue<T: LfsObject> {
    v: T,
    state: T::State,
}

impl<T: LfsObject> ProtocolObjectState for LfsValue<T> {
    type Value = T;
    fn into_value(self) -> Self::Value {
        self.v
    }
    fn initialize(decoder: &Decoder) -> Self {
        let (v, state) = T::init_state(decoder);
        Self { v, state }
    }
    fn complete(
        mut self,
        decoder: &mut Decoder,
    ) -> ProtocolResult<ProtocolObjectDecodeState<Self>> {
        let mut stop = decoder.cursor_eq(b'\n');
        let mut error = false;
        while (!decoder.eof()) && (!error) && (!stop) {
            let byte = decoder.next();
            error = !self.v.update(&mut self.state, byte);
            stop = decoder.cursor_eq(b'\n');
        }
        if stop & !error {
            decoder.i += 1; // account for LF
            let Self { state, v } = self;
            match v.complete_lfs(&state, &decoder) {
                Ok(v) => Ok(ProtocolObjectDecodeState::Completed(Self { v, state })),
                Err(e) => Err(e),
            }
        } else {
            if error {
                Err(ProtocolError::InvalidServerResponseForData)
            } else {
                Ok(ProtocolObjectDecodeState::Pending(self))
            }
        }
    }
}

/*
    lfs objects with no state mgmt
*/

macro_rules! impl_num_lfs_object {
    ($($ty:ty),*) => {
        $(
            impl LfsObject for $ty {
                type State = ();
                fn init_state(_: &Decoder) -> (Self, Self::State) {(0, ())}
                fn update(&mut self, _: &mut Self::State, byte: u8) -> bool {
                    match self.checked_mul(10).map(|me| me.checked_add((byte & 0x0f) as $ty)) {
                        Some(Some(v)) if byte.is_ascii_digit() => { *self = v; true },
                        _ => false,
                    }
                }
            }
        )*
    };
}

impl_num_lfs_object!(u8, u16, u32, u64, usize);

/*
    lfs objects requiring state mgmt
*/

#[derive(Debug, PartialEq)]
pub(crate) struct LfsObjectState {
    start: usize,
}

macro_rules! impl_num_lfs_object_state {
    ($($ty:ty),*) => {
        $(
            impl LfsObject for $ty {
                type State = LfsObjectState;
                fn init_state(decoder: &Decoder) -> (Self, Self::State) { (<$ty as ::core::default::Default>::default(), LfsObjectState { start: decoder.i},) }
                fn update(&mut self, _: &mut Self::State, _: u8) -> bool { true }
                fn complete_lfs(self, state: &Self::State, decoder: &Decoder) -> ProtocolResult<Self> {
                    let block = &decoder.b[state.start..decoder.i-1]; // -1 for LF
                    match core::str::from_utf8(block).map(str::parse) {
                        Ok(Ok(v)) => Ok(v),
                        _ => Err(ProtocolError::InvalidServerResponseForData),
                    }
                }
            }
        )*
    };
}

impl_num_lfs_object_state!(f32, f64, i8, i16, i32, i64, isize);

/*
    spobjects: binary, string
*/

trait SpObject: Sized {
    fn finish(block: &[u8]) -> ProtocolResult<Self>;
    fn init() -> Self;
}

#[derive(Debug, PartialEq)]
pub(crate) struct SpObjectState<T> {
    size: ProtocolObjectDecodeState<usize, LfsValue<usize>>,
    v: T,
    _d: PhantomData<T>,
}

impl<T: SpObject> ProtocolObjectState for SpObjectState<T> {
    type Value = T;
    fn initialize(decoder: &Decoder) -> Self {
        Self {
            size: ProtocolObjectDecodeState::Pending(LfsValue::initialize(decoder)),
            v: T::init(),
            _d: PhantomData,
        }
    }
    fn into_value(self) -> Self::Value {
        self.v
    }
    fn complete(
        mut self,
        decoder: &mut Decoder,
    ) -> ProtocolResult<ProtocolObjectDecodeState<Self>> {
        let size = match self.size.try_complete(decoder)? {
            ProtocolObjectDecodeState::Completed(c) => c,
            ProtocolObjectDecodeState::Pending(pv) => {
                self.size = ProtocolObjectDecodeState::Pending(pv);
                return Ok(ProtocolObjectDecodeState::Pending(self));
            }
        };
        self.size = ProtocolObjectDecodeState::Completed(size);
        if decoder.has_left(size) {
            let block = decoder.next_chunk(size);
            let v = T::finish(block)?;
            self.v = v;
            Ok(ProtocolObjectDecodeState::Completed(self))
        } else {
            Ok(ProtocolObjectDecodeState::Pending(self))
        }
    }
}

impl SpObject for Vec<u8> {
    fn init() -> Self {
        vec![]
    }
    fn finish(block: &[u8]) -> ProtocolResult<Self> {
        Ok(block.to_owned())
    }
}

impl SpObject for String {
    fn init() -> Self {
        String::new()
    }
    fn finish(block: &[u8]) -> ProtocolResult<Self> {
        if core::str::from_utf8(block).is_ok() {
            Ok(unsafe { String::from_utf8_unchecked(block.to_owned()) })
        } else {
            Err(ProtocolError::InvalidServerResponseForData)
        }
    }
}

#[derive(Debug, PartialEq)]
pub(crate) enum PendingValue {
    Bool(bool),
    UInt8(LfsValue<u8>),
    UInt16(LfsValue<u16>),
    UInt32(LfsValue<u32>),
    UInt64(LfsValue<u64>),
    SInt8(LfsValue<i8>),
    SInt16(LfsValue<i16>),
    SInt32(LfsValue<i32>),
    SInt64(LfsValue<i64>),
    Float32(LfsValue<f32>),
    Float64(LfsValue<f64>),
    Binary(SpObjectState<Vec<u8>>),
    String(SpObjectState<String>),
    List(ValueStream),
}

macro_rules! translate_pending_lfs {
    ($($base:ident => {$($type:ty as $variant:ident),*}),* $(,)?) => {
        $($(
            impl From<$base<$type>> for PendingValue {
                fn from(t: $base<$type>) -> Self {
                    PendingValue::$variant(t)
                }
            }
            impl From<$base<$type>> for Value {
                fn from(t: $base<$type>) -> Value {
                    Value::$variant(t.into_value())
                }
            }
        )*)*
    }
}

translate_pending_lfs!(
    LfsValue => {
        u8 as UInt8, u16 as UInt16, u32 as UInt32, u64 as UInt64, i8 as SInt8, i16 as SInt16, i32 as SInt32, i64 as SInt64, f32 as Float32, f64 as Float64
    },
    SpObjectState => {Vec<u8> as Binary, String as String},
);

impl From<ValueStream> for PendingValue {
    fn from(value: ValueStream) -> Self {
        Self::List(value)
    }
}

impl From<ValueStream> for Value {
    fn from(value: ValueStream) -> Self {
        Self::List(value.items)
    }
}

impl PendingValue {
    fn next_value_with_code(
        decoder: &mut Decoder,
        code: u8,
    ) -> ProtocolResult<ProtocolObjectDecodeState<Value, PendingValue>> {
        match code {
            0x00 => Ok(ProtocolObjectDecodeState::Completed(Value::Null)),
            0x01 => Self::decode_bool(decoder),
            0x02 => Self::try_value::<LfsValue<u8>>(decoder),
            0x03 => Self::try_value::<LfsValue<u16>>(decoder),
            0x04 => Self::try_value::<LfsValue<u32>>(decoder),
            0x05 => Self::try_value::<LfsValue<u64>>(decoder),
            0x06 => Self::try_value::<LfsValue<i8>>(decoder),
            0x07 => Self::try_value::<LfsValue<i16>>(decoder),
            0x08 => Self::try_value::<LfsValue<i32>>(decoder),
            0x09 => Self::try_value::<LfsValue<i64>>(decoder),
            0x0A => Self::try_value::<LfsValue<f32>>(decoder),
            0x0B => Self::try_value::<LfsValue<f64>>(decoder),
            0x0C => Self::try_value::<SpObjectState<Vec<u8>>>(decoder),
            0x0D => Self::try_value::<SpObjectState<String>>(decoder),
            0x0E => Self::try_value::<ValueStream>(decoder),
            _ => Err(ProtocolError::InvalidServerResponseUnknownDataType),
        }
    }
    fn next_value(
        decoder: &mut Decoder,
    ) -> ProtocolResult<ProtocolObjectDecodeState<Value, PendingValue>> {
        let code = decoder.next();
        Self::next_value_with_code(decoder, code)
    }
    fn try_complete_self(
        self,
        decoder: &mut Decoder,
    ) -> ProtocolResult<ProtocolObjectDecodeState<Value, PendingValue>> {
        match self {
            PendingValue::Bool(_) => Self::decode_bool(decoder),
            PendingValue::UInt8(pv) => Self::complete_value(pv, decoder),
            PendingValue::UInt16(pv) => Self::complete_value(pv, decoder),
            PendingValue::UInt32(pv) => Self::complete_value(pv, decoder),
            PendingValue::UInt64(pv) => Self::complete_value(pv, decoder),
            PendingValue::SInt8(pv) => Self::complete_value(pv, decoder),
            PendingValue::SInt16(pv) => Self::complete_value(pv, decoder),
            PendingValue::SInt32(pv) => Self::complete_value(pv, decoder),
            PendingValue::SInt64(pv) => Self::complete_value(pv, decoder),
            PendingValue::Float32(pv) => Self::complete_value(pv, decoder),
            PendingValue::Float64(pv) => Self::complete_value(pv, decoder),
            PendingValue::Binary(pv) => Self::complete_value(pv, decoder),
            PendingValue::String(pv) => Self::complete_value(pv, decoder),
            PendingValue::List(pv) => Self::complete_value(pv, decoder),
        }
    }
    fn complete_value<T: ProtocolObjectState + Into<Value> + Into<PendingValue>>(
        current: T,
        decoder: &mut Decoder,
    ) -> ProtocolResult<ProtocolObjectDecodeState<Value, PendingValue>> {
        match current.complete(decoder)? {
            ProtocolObjectDecodeState::Completed(c) => {
                Ok(ProtocolObjectDecodeState::Completed(c.into()))
            }
            ProtocolObjectDecodeState::Pending(p) => {
                Ok(ProtocolObjectDecodeState::Pending(p.into()))
            }
        }
    }
    fn try_value<T: ProtocolObjectState + Into<Value> + Into<PendingValue>>(
        decoder: &mut Decoder,
    ) -> ProtocolResult<ProtocolObjectDecodeState<Value, PendingValue>> {
        Self::complete_value(T::initialize(decoder), decoder)
    }
    fn decode_bool(
        decoder: &mut Decoder,
    ) -> Result<ProtocolObjectDecodeState<Value, PendingValue>, ProtocolError> {
        // bool
        if !decoder.eof() {
            let value = decoder.next();
            if value > 1 {
                Err(ProtocolError::InvalidServerResponseForData)
            } else {
                Ok(ProtocolObjectDecodeState::Completed(Value::Bool(
                    value == 1,
                )))
            }
        } else {
            Ok(ProtocolObjectDecodeState::Pending(PendingValue::Bool(
                false,
            )))
        }
    }
}

/*
    value stream: a sequential list of values (state cached)
*/

pub(crate) trait AsValueStream: Sized {
    fn from_value_stream(v: Vec<Value>) -> Self;
}

impl AsValueStream for Row {
    fn from_value_stream(v: Vec<Value>) -> Self {
        Row::new(v)
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct ValueStream {
    element_count: ProtocolObjectDecodeState<usize, LfsValue<usize>>,
    items: Vec<Value>,
    pending: Option<Box<PendingValue>>,
}

impl ValueStream {
    fn _complete(
        mut self,
        decoder: &mut Decoder,
        size: usize,
    ) -> ProtocolResult<ProtocolObjectDecodeState<Self>> {
        while self.items.len() != size {
            if decoder.eof() {
                return Ok(ProtocolObjectDecodeState::Pending(self));
            }
            let r = match self.pending.take() {
                Some(v) => v.try_complete_self(decoder),
                None => PendingValue::next_value(decoder),
            }?;
            match r {
                ProtocolObjectDecodeState::Completed(v) => {
                    self.items.push(v);
                }
                ProtocolObjectDecodeState::Pending(pv) => {
                    self.pending = Some(Box::new(pv));
                    return Ok(ProtocolObjectDecodeState::Pending(self));
                }
            }
        }
        Ok(ProtocolObjectDecodeState::Completed(self))
    }
}

impl ProtocolObjectState for ValueStream {
    type Value = Vec<Value>;
    fn initialize(decoder: &Decoder) -> Self {
        Self {
            element_count: ProtocolObjectDecodeState::Pending(LfsValue::initialize(decoder)),
            items: vec![],
            pending: None,
        }
    }
    fn complete(
        mut self,
        decoder: &mut Decoder,
    ) -> ProtocolResult<ProtocolObjectDecodeState<Self>> {
        let size = match self.element_count.try_complete(decoder)? {
            ProtocolObjectDecodeState::Completed(c) => c,
            ProtocolObjectDecodeState::Pending(pv) => {
                self.element_count = ProtocolObjectDecodeState::Pending(pv);
                return Ok(ProtocolObjectDecodeState::Pending(self));
            }
        };
        self.element_count = ProtocolObjectDecodeState::Completed(size);
        self._complete(decoder, size)
    }
    fn into_value(self) -> Self::Value {
        self.items
    }
}

/*
    multi value stream: sequential collection of value streams
*/

#[derive(Debug, PartialEq)]
pub(crate) struct MultiValueStream {
    stream_count: ProtocolObjectDecodeState<usize, LfsValue<usize>>,
    stream_size: ProtocolObjectDecodeState<usize, LfsValue<usize>>,
    items: Vec<Vec<Value>>,
    pending: Option<ValueStream>,
}

impl ProtocolObjectState for MultiValueStream {
    type Value = Vec<Vec<Value>>;
    fn initialize(decoder: &Decoder) -> Self {
        Self {
            stream_count: ProtocolObjectDecodeState::Pending(LfsValue::initialize(decoder)),
            stream_size: ProtocolObjectDecodeState::Pending(LfsValue::initialize(decoder)),
            items: vec![],
            pending: None,
        }
    }
    fn complete(
        mut self,
        decoder: &mut Decoder,
    ) -> ProtocolResult<ProtocolObjectDecodeState<Self>> {
        // get number of streams
        let stream_count = match self.stream_count.try_complete(decoder)? {
            ProtocolObjectDecodeState::Completed(sz) => sz,
            ProtocolObjectDecodeState::Pending(pv) => {
                self.stream_count = ProtocolObjectDecodeState::Pending(pv);
                return Ok(ProtocolObjectDecodeState::Pending(self));
            }
        };
        self.stream_count = ProtocolObjectDecodeState::Completed(stream_count);
        // get per stream size
        let stream_size = match self.stream_size.try_complete(decoder)? {
            ProtocolObjectDecodeState::Completed(sz) => sz,
            ProtocolObjectDecodeState::Pending(pv) => {
                self.stream_size = ProtocolObjectDecodeState::Pending(pv);
                return Ok(ProtocolObjectDecodeState::Pending(self));
            }
        };
        self.stream_size = ProtocolObjectDecodeState::Completed(stream_size);
        // load items
        while self.items.len() != stream_count {
            match match self.pending.take() {
                Some(pending_vs) => pending_vs._complete(decoder, stream_size),
                None => ValueStream::initialize(decoder)._complete(decoder, stream_size),
            }? {
                ProtocolObjectDecodeState::Completed(vs) => {
                    self.items.push(vs.items);
                }
                ProtocolObjectDecodeState::Pending(pvs) => {
                    self.pending = Some(pvs);
                    return Ok(ProtocolObjectDecodeState::Pending(self));
                }
            }
        }
        Ok(ProtocolObjectDecodeState::Completed(self))
    }
    fn into_value(self) -> Self::Value {
        self.items
    }
}

#[test]
fn decode_lfs_object() {
    {
        let b = b"-3.142\n";
        for i in 1..b.len() {
            let mut decoder = Decoder::new(&b[..i], 0);
            assert!(matches!(
                LfsValue::<f32>::initialize(&decoder)
                    .complete(&mut decoder)
                    .unwrap(),
                ProtocolObjectDecodeState::Pending(_)
            ))
        }
        let mut decoder = Decoder::new(b, 0);
        assert_eq!(
            LfsValue::<f32>::initialize(&decoder)
                .complete(&mut decoder)
                .unwrap()
                .into_completed()
                .unwrap()
                .into_value(),
            -3.142_f32
        );
    }
    {
        let b = b"1096\n";
        for i in 1..b.len() {
            let mut decoder = Decoder::new(&b[..i], 0);
            assert!(matches!(
                LfsValue::<u16>::initialize(&decoder)
                    .complete(&mut decoder)
                    .unwrap(),
                ProtocolObjectDecodeState::Pending(_)
            ))
        }
        let mut decoder = Decoder::new(b, 0);
        assert_eq!(
            LfsValue::<u16>::initialize(&decoder)
                .complete(&mut decoder)
                .unwrap()
                .into_completed()
                .unwrap()
                .into_value(),
            1096u16
        );
    }
    {
        let b = b"-1032\n";
        for i in 1..b.len() {
            let mut decoder = Decoder::new(&b[..i], 0);
            assert!(matches!(
                LfsValue::<i16>::initialize(&decoder)
                    .complete(&mut decoder)
                    .unwrap(),
                ProtocolObjectDecodeState::Pending(_)
            ))
        }
        let mut decoder = Decoder::new(b, 0);
        assert_eq!(
            LfsValue::<i16>::initialize(&decoder)
                .complete(&mut decoder)
                .unwrap()
                .into_completed()
                .unwrap()
                .into_value(),
            -1032i16
        );
    }
}

#[test]
fn decode_sp_object() {
    {
        let b = b"5\nhello";
        for i in 1..b.len() {
            let mut decoder = Decoder::new(&b[..i], 0);
            assert!(matches!(
                SpObjectState::<Vec<u8>>::initialize(&decoder)
                    .complete(&mut decoder)
                    .unwrap(),
                ProtocolObjectDecodeState::Pending(_)
            ))
        }
        let mut decoder = Decoder::new(b, 0);
        assert_eq!(
            SpObjectState::<Vec<u8>>::initialize(&decoder)
                .complete(&mut decoder)
                .unwrap()
                .into_completed()
                .unwrap()
                .into_value(),
            b"hello"
        );
    }
    {
        let b = b"6\nworld!";
        for i in 1..b.len() {
            let mut decoder = Decoder::new(&b[..i], 0);
            assert!(matches!(
                SpObjectState::<String>::initialize(&decoder)
                    .complete(&mut decoder)
                    .unwrap(),
                ProtocolObjectDecodeState::Pending(_)
            ))
        }
        let mut decoder = Decoder::new(b, 0);
        assert_eq!(
            SpObjectState::<String>::initialize(&decoder)
                .complete(&mut decoder)
                .unwrap()
                .into_completed()
                .unwrap()
                .into_value(),
            "world!"
        );
    }
}

#[test]
fn decode_value_stream() {
    // [null, bool, uint, sint, float, binary, string, [binary, string]]
    const QUERY: &[u8] = b"8\n\x00\x01\x01\x0518446744073709551615\n\x09-9223372036854775808\n\x0A-3.141592654\n\x0C5\nabcde\x0D5\nfghij\x0E2\n\x0C5\nabcde\x0D5\nfghij";
    for i in 1..QUERY.len() {
        let block = &QUERY[..i];
        let mut decoder = Decoder::new(block, 0);
        assert!(matches!(
            ValueStream::initialize(&decoder)
                .complete(&mut decoder)
                .unwrap(),
            ProtocolObjectDecodeState::Pending(_)
        ));
    }
    let mut decoder = Decoder::new(QUERY, 0);
    assert_eq!(
        ValueStream::initialize(&decoder)
            .complete(&mut decoder)
            .unwrap()
            .into_completed()
            .unwrap()
            .into_value(),
        vec![
            Value::Null,
            Value::Bool(true),
            Value::UInt64(u64::MAX),
            Value::SInt64(i64::MIN),
            Value::Float32(-3.141592654),
            Value::Binary(b"abcde".to_vec()),
            Value::String("fghij".to_string()),
            Value::List(vec![
                Value::Binary(b"abcde".to_vec()),
                Value::String("fghij".to_string())
            ])
        ]
    );
}

#[test]
fn decode_multi_value_stream() {
    let packet = [
        b"5\n8\n".to_vec(),
        "\x00\x01\x01\x0518446744073709551615\n\x09-9223372036854775808\n\x0A-3.141592654\n\x0C5\nabcde\x0D5\nfghij\x0E2\n\x0C5\nabcde\x0D5\nfghij".repeat(5).into_bytes()
    ].concat();
    for i in 1..packet.len() {
        let mut decoder = Decoder::new(&packet[..i], 0);
        assert!(matches!(
            MultiValueStream::initialize(&decoder)
                .complete(&mut decoder)
                .unwrap(),
            ProtocolObjectDecodeState::Pending(_)
        ))
    }
    let mut decoder = Decoder::new(&packet, 0);
    assert_eq!(
        MultiValueStream::initialize(&decoder)
            .complete(&mut decoder)
            .unwrap()
            .into_completed()
            .unwrap()
            .into_value(),
        (0..5)
            .map(|_| vec![
                Value::Null,
                Value::Bool(true),
                Value::UInt64(u64::MAX),
                Value::SInt64(i64::MIN),
                Value::Float32(-3.141592654),
                Value::Binary(b"abcde".to_vec()),
                Value::String("fghij".to_string()),
                Value::List(vec![
                    Value::Binary(b"abcde".to_vec()),
                    Value::String("fghij".to_string())
                ])
            ])
            .collect::<Vec<_>>()
    );
}
