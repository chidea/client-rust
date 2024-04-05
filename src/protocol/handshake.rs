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

use crate::{
    error::{ConnectionSetupError, Error},
    ClientResult, Config,
};

#[derive(Debug, PartialEq, Clone, Copy)]
#[repr(u8)]
/// The Skyhash protocol version
pub enum ProtocolVersion {
    /// Skyhash 2.0
    V2_0,
}

impl ProtocolVersion {
    pub(crate) const fn hs_block(&self) -> [u8; 6] {
        match self {
            Self::V2_0 => [b'H', 0, 0, 0, 0, 0],
        }
    }
}

pub struct ClientHandshake(Box<[u8]>);
impl ClientHandshake {
    pub(crate) fn new(cfg: &Config) -> Self {
        Self::_new(cfg.protocol().hs_block(), cfg)
    }
    fn _new(hs: [u8; 6], cfg: &Config) -> Self {
        let mut v = Vec::with_capacity(6 + cfg.username().len() + cfg.password().len() + 5);
        v.extend(hs);
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
