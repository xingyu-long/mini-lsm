// REMOVE THIS LINE after fully implementing this functionality
// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use anyhow::{Context, Result, bail};
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use crate::key::{KeyBytes, KeySlice};

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(_path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(
                OpenOptions::new()
                    .read(true)
                    .create_new(true)
                    .write(true)
                    .open(_path)
                    .context("failed to create wal file")?,
            ))),
        })
    }

    pub fn recover(_path: impl AsRef<Path>, _skiplist: &SkipMap<KeyBytes, Bytes>) -> Result<Self> {
        let path = _path.as_ref();
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path)
            .context("failed to open wal file")?;

        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut rbuf = buf.as_slice();
        while rbuf.has_remaining() {
            let mut checksum_buf = Vec::new();
            let batch_size = rbuf.get_u32() as usize;
            let mut body_rbuf = &rbuf[..batch_size];
            checksum_buf.extend(body_rbuf);
            let mut entries = Vec::new();

            while body_rbuf.has_remaining() {
                let key_len = body_rbuf.get_u16() as usize;
                let key = Bytes::copy_from_slice(&body_rbuf[..key_len]);
                body_rbuf.advance(key_len);
                let ts = body_rbuf.get_u64();

                let value_len = body_rbuf.get_u16() as usize;
                let value = Bytes::copy_from_slice(&body_rbuf[..value_len]);
                body_rbuf.advance(value_len);

                entries.push((KeyBytes::from_bytes_with_ts(key, ts), value));
            }
            rbuf.advance(batch_size);

            let checksum = rbuf.get_u32();
            if checksum != crc32fast::hash(&checksum_buf) {
                bail!("checksum doesn't match!");
            }

            // insert everything from entries to _skiplist since it passed checksum check
            for entry in entries {
                _skiplist.insert(entry.0, entry.1);
            }
        }
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    // week 2, day 6
    // | key_len (exclude ts len) (u16) | key | ts (u64) | value_len (u16) | value | checksum (u32) |
    pub fn put(&self, _key: KeySlice, _value: &[u8]) -> Result<()> {
        self.put_batch(&[(_key, _value)])
    }

    /// Implement this in week 3, day 5; if you want to implement this earlier, use `&[u8]` as the key type.
    // week 3, day 5: Atomic WAL
    // |   HEADER   |                          BODY                                      |  FOOTER  |
    // |     u32    |   u16   | var | u64 |    u16    |  var  |           ...            |    u32   |
    // | batch_size | key_len | key | ts  | value_len | value | more key-value pairs ... | checksum |
    pub fn put_batch(&self, _data: &[(KeySlice, &[u8])]) -> Result<()> {
        let mut file = self.file.lock();
        let mut buf: Vec<u8> = Vec::new();
        let mut body_buf: Vec<u8> = Vec::new();
        // prepare body
        for (key, value) in _data {
            // key
            body_buf.put_u16(key.key_len() as u16);
            body_buf.put(key.key_ref());
            body_buf.put_u64(key.ts());
            // value
            body_buf.put_u16(value.len() as u16);
            body_buf.put(*value);
        }
        let checksum = crc32fast::hash(&body_buf);
        // header
        buf.put_u32(body_buf.len() as u32);
        // body
        buf.extend(body_buf);
        // footer
        buf.put_u32(checksum);

        file.write_all(&buf)?;
        file.flush().unwrap();

        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.get_mut().sync_all()?;
        Ok(())
    }
}
