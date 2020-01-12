use std::io::{Read, Error};
use crate::pg_alloc::PgAllocator;
use crate::pg_sys;
use std::convert::TryInto;

#[derive(Clone)]
pub struct LargeObject<'a> {
    lod : *mut pg_sys::LargeObjectDesc,
    _alloc : &'a PgAllocator
}

impl <'a> LargeObject<'a> {
    pub fn new(_alloc: &'a PgAllocator, lo_id : u32) -> Result<LargeObject<'a>, Error> {
        unsafe {
            let lod : *mut pg_sys::LargeObjectDesc = {
                _alloc.exec_with_guard(|| pg_sys::inv_open(lo_id, 0x20000, pg_sys::CurrentMemoryContext) )
            };
            Ok(LargeObject { lod, _alloc })
        }
    }
}

impl <'a> Read for LargeObject<'a> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, Error> {
        let buf_ptr = buf.as_mut_ptr() as *mut i8;
        let read = unsafe {
            self._alloc.exec_with_guard(|| {
                pg_sys::inv_read(self.lod, buf_ptr, buf.len().try_into().unwrap())
            })
        };
        Ok(read.try_into().unwrap())
    }
}

impl <'a> Drop for LargeObject<'a> {
    fn drop(&mut self) {
        unsafe {
            pg_sys::inv_close(self.lod);
        }
    }
}