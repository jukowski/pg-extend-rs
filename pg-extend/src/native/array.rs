// Copyright 2018-2019 Benjamin Fry <benjaminfry@me.com>
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

use std::ffi::CString;
use std::ptr::NonNull;

use crate::native::VarLenA;
use crate::pg_alloc::{PgAllocated, PgAllocator};
use crate::pg_sys;
use crate::pg_datum::{PgDatum, TryFromPgDatum, PgPrimitiveDatum};
use crate::pg_type::{PgTypeInfo, PgType};

/// A zero-overhead view of `text` data from Postgres
pub struct Array<'mc>(PgAllocated<'mc, NonNull<pg_sys::varlena>>);

impl<'mc> Array<'mc> {
    /// Create from the raw pointer to the Postgres data
    #[allow(clippy::missing_safety_doc)]
    pub unsafe fn from_raw(alloc: &'mc PgAllocator, bytea_ptr: *mut pg_sys::varlena) -> Self {
        Array(PgAllocated::from_raw(alloc, bytea_ptr))
    }

    /// Convert into the underlying pointer
    #[allow(clippy::missing_safety_doc)]
    pub unsafe fn into_ptr(mut self) -> *mut pg_sys::text {
        self.0.take_ptr()
    }


    /// Allocate a new ByteA data from a u8 slice using the PgAllocator for the Postgres MemoryContext
    pub fn from_slice<T>(alloc: &'mc PgAllocator, s: &[T]) -> Self where T : PgPrimitiveDatum + PgTypeInfo + Clone {
        unsafe {
            alloc.exec_with_guard(|| {
                let oid = match T::pg_type() {
                    PgType::Int4 => pg_sys::INT4OID,
                    PgType::Text => pg_sys::TEXTOID,
                    _ => 0
                };
                let mut astate : *mut pg_sys::ArrayBuildState = pg_sys::initArrayResult(oid, pg_sys::CurrentMemoryContext, false);
                for item in s {
                    let pg_item = PgDatum::from(item.clone());
                    astate = pg_sys::accumArrayResult(astate, pg_item.into_datum(), false, oid, pg_sys::CurrentMemoryContext);
                }
                let bytea_ptr = pg_sys::makeArrayResult(astate, pg_sys::CurrentMemoryContext) as *mut pg_sys::bytea;
                Array::from_raw(alloc, bytea_ptr)
            })
        }
    }

    /// Allocate a new ByteA data from a u8 slice using the PgAllocator for the Postgres MemoryContext
    pub fn from_i32_slice(alloc: &'mc PgAllocator, s: &[i32]) -> Self {
        unsafe {
            alloc.exec_with_guard(|| {

                let mut astate : *mut pg_sys::ArrayBuildState = pg_sys::initArrayResult(pg_sys::INT4OID, pg_sys::CurrentMemoryContext, false);
                for item in s {

                    let pg_item = PgDatum::from(*item);
                    astate = pg_sys::accumArrayResult(astate, pg_item.into_datum(), false, pg_sys::INT4OID, pg_sys::CurrentMemoryContext);
                }
                let bytea_ptr = pg_sys::makeArrayResult(astate, pg_sys::CurrentMemoryContext) as *mut pg_sys::bytea;
                Array::from_raw(alloc, bytea_ptr)
            })
        }
    }

    /// Allocate a new ByteA data from a u8 slice using the PgAllocator for the Postgres MemoryContext
    pub fn from_string_slice(alloc: &'mc PgAllocator, s: &[String]) -> Self {
        unsafe {
            alloc.exec_with_guard(|| {

                let mut astate : *mut pg_sys::ArrayBuildState = pg_sys::initArrayResult(pg_sys::TEXTOID, pg_sys::CurrentMemoryContext, false);
                for item in s {

                    let pg_item = PgDatum::from(item.clone());
                    astate = pg_sys::accumArrayResult(astate, pg_item.into_datum(), false, pg_sys::TEXTOID, pg_sys::CurrentMemoryContext);
                }
                let bytea_ptr = pg_sys::makeArrayResult(astate, pg_sys::CurrentMemoryContext) as *mut pg_sys::bytea;
                Array::from_raw(alloc, bytea_ptr)
            })
        }
    }

    /// Return true if this is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return the length of the text data
    pub fn len(&self) -> usize {
        let varlena = unsafe { VarLenA::from_varlena(self.0.as_ref()) };
        varlena.len()
    }
}

/*
/// *WARNING* This requires the database to be a UTF-8 locale.
impl<'mc, T> Deref for Array<'mc, T> {
    type Target = str;

    fn deref(&self) -> &str {
        // FIXME: this should panic if the DB is not UTF-8.
        unsafe {
            let varlena = VarLenA::from_varlena(self.0.as_ref());
            str::from_utf8_unchecked(&*(varlena.as_slice() as *const [i8] as *const [u8]))
        }
    }
}

*/