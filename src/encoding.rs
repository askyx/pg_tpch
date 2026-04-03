use std::{fmt, ptr};

use pgrx::pg_sys;
use tpchgen::{dates::TPCHDate, decimal::TPCHDecimal};

const VARHDRSZ: usize = std::mem::size_of::<i32>();
const POSTGRES_EPOCH_OFFSET_FROM_UNIX_DAYS: i32 = 10_957;

const NUMERIC_POS: u16 = 0x0000;
const NUMERIC_NEG: u16 = 0x4000;
const NUMERIC_SHORT: u16 = 0x8000;

const NUMERIC_DSCALE_MASK: u16 = 0x3FFF;
const NUMERIC_SHORT_SIGN_MASK: u16 = 0x2000;
const NUMERIC_SHORT_DSCALE_SHIFT: u16 = 7;
const NUMERIC_SHORT_DSCALE_MAX: u16 = 0x1F80 >> NUMERIC_SHORT_DSCALE_SHIFT;
const NUMERIC_SHORT_WEIGHT_SIGN_MASK: u16 = 0x0040;
const NUMERIC_SHORT_WEIGHT_MASK: u16 = 0x003F;
const NUMERIC_SHORT_WEIGHT_MAX: i16 = NUMERIC_SHORT_WEIGHT_MASK as i16;
const NUMERIC_SHORT_WEIGHT_MIN: i16 = -((NUMERIC_SHORT_WEIGHT_MASK as i16) + 1);

pub(crate) fn text_datum_str(value: &str) -> pg_sys::Datum {
    unsafe { varlena_datum_from_bytes(&encode_text_bytes(value)) }
}

pub(crate) fn text_datum(value: impl fmt::Display) -> pg_sys::Datum {
    unsafe { varlena_datum_from_bytes(&encode_text_display_bytes(value)) }
}

pub(crate) fn integer_numeric_datum(value: i64) -> pg_sys::Datum {
    scaled_numeric_datum(value, 0)
}

pub(crate) fn decimal_numeric_datum(value: TPCHDecimal) -> pg_sys::Datum {
    scaled_numeric_datum(value.into_inner(), 2)
}

pub(crate) fn scaled_numeric_datum(value: i64, dscale: u16) -> pg_sys::Datum {
    unsafe { varlena_datum_from_bytes(&encode_numeric_i64_with_scale_bytes(value, dscale)) }
}

pub(crate) fn date_datum(value: TPCHDate) -> pg_sys::Datum {
    pg_sys::Datum::from(tpch_date_to_pg_date(value))
}

pub(crate) fn tpch_date_to_pg_date(value: TPCHDate) -> pg_sys::DateADT {
    value.to_unix_epoch() - POSTGRES_EPOCH_OFFSET_FROM_UNIX_DAYS
}

#[allow(dead_code)]
pub(crate) fn encode_text_bytes(value: &str) -> Vec<u8> {
    let mut bytes: Vec<u8> = Vec::with_capacity(VARHDRSZ + value.len());
    bytes.resize(VARHDRSZ, 0);
    bytes.extend_from_slice(value.as_bytes());
    set_varlena_header(&mut bytes);
    bytes
}

pub(crate) fn encode_text_display_bytes(value: impl fmt::Display) -> Vec<u8> {
    let mut writer = TextVarlenaWriter {
        bytes: Vec::with_capacity(VARHDRSZ + 32),
    };
    writer.bytes.resize(VARHDRSZ, 0);
    fmt::write(&mut writer, format_args!("{value}")).expect("writing to Vec<u8> cannot fail");
    set_varlena_header(&mut writer.bytes);
    writer.bytes
}

#[allow(dead_code)]
pub(crate) fn encode_numeric_i64_bytes(value: i64) -> Vec<u8> {
    encode_numeric_i64_with_scale_bytes(value, 0)
}

#[allow(dead_code)]
pub(crate) fn encode_numeric_i64_with_dscale_2_bytes(value: i64) -> Vec<u8> {
    encode_numeric_i64_with_scale_bytes(value, 2)
}

pub(crate) fn encode_numeric_i64_with_scale_bytes(value: i64, dscale: u16) -> Vec<u8> {
    let sign = if value < 0 { NUMERIC_NEG } else { NUMERIC_POS };
    let (mut digits, mut weight) = scaled_base_10000_digits(value.unsigned_abs(), dscale);
    trim_leading_numeric_zeroes(&mut digits, &mut weight);
    trim_trailing_numeric_zeroes(&mut digits);
    encode_numeric_bytes(
        sign,
        dscale,
        if digits.is_empty() { 0 } else { weight },
        &digits,
    )
}

unsafe fn varlena_datum_from_bytes(bytes: &[u8]) -> pg_sys::Datum {
    let ptr = pg_sys::palloc(bytes.len()) as *mut u8;
    ptr::copy_nonoverlapping(bytes.as_ptr(), ptr, bytes.len());
    pg_sys::Datum::from(ptr as usize)
}

struct TextVarlenaWriter {
    bytes: Vec<u8>,
}

impl fmt::Write for TextVarlenaWriter {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        self.bytes.extend_from_slice(s.as_bytes());
        Ok(())
    }
}

fn base_10000_digits(mut value: u64) -> Vec<i16> {
    if value == 0 {
        return Vec::new();
    }

    let mut digits = Vec::new();
    while value > 0 {
        digits.push((value % 10_000) as i16);
        value /= 10_000;
    }
    digits.reverse();
    digits
}

fn scaled_base_10000_digits(value: u64, dscale: u16) -> (Vec<i16>, i16) {
    if dscale == 0 {
        let digits = base_10000_digits(value);
        let weight = if digits.is_empty() {
            0
        } else {
            digits.len() as i16 - 1
        };
        return (digits, weight);
    }

    let mut remaining = value;
    let mut fractional_groups_rev = Vec::with_capacity(usize::from(dscale.div_ceil(4)));
    let mut remaining_scale = dscale;
    let last_group_digits = ((dscale - 1) % 4) + 1;

    let least_significant_divisor = 10_u64.pow(u32::from(last_group_digits));
    fractional_groups_rev.push(
        ((remaining % least_significant_divisor) * 10_u64.pow(u32::from(4 - last_group_digits)))
            as i16,
    );
    remaining /= least_significant_divisor;
    remaining_scale -= last_group_digits;

    while remaining_scale > 0 {
        fractional_groups_rev.push((remaining % 10_000) as i16);
        remaining /= 10_000;
        remaining_scale -= 4;
    }

    let mut digits = base_10000_digits(remaining);
    let weight = if digits.is_empty() {
        -1
    } else {
        digits.len() as i16 - 1
    };

    digits.extend(fractional_groups_rev.into_iter().rev());
    (digits, weight)
}

fn trim_leading_numeric_zeroes(digits: &mut Vec<i16>, weight: &mut i16) {
    let leading_zeroes = digits.iter().take_while(|digit| **digit == 0).count();
    if leading_zeroes == 0 {
        return;
    }

    if leading_zeroes == digits.len() {
        digits.clear();
        *weight = 0;
        return;
    }

    digits.drain(..leading_zeroes);
    *weight -= leading_zeroes as i16;
}

fn trim_trailing_numeric_zeroes(digits: &mut Vec<i16>) {
    while digits.last() == Some(&0) {
        digits.pop();
    }
}

fn encode_numeric_bytes(sign: u16, dscale: u16, weight: i16, digits: &[i16]) -> Vec<u8> {
    let can_be_short = dscale <= NUMERIC_SHORT_DSCALE_MAX
        && (NUMERIC_SHORT_WEIGHT_MIN..=NUMERIC_SHORT_WEIGHT_MAX).contains(&weight);
    let header_size = if can_be_short { 2 } else { 4 };
    let total_len = VARHDRSZ + header_size + digits.len() * std::mem::size_of::<i16>();

    let mut bytes = Vec::with_capacity(total_len);
    bytes.extend_from_slice(&encode_varlena_header(total_len));

    if can_be_short {
        let short_header = (if sign == NUMERIC_NEG {
            NUMERIC_SHORT | NUMERIC_SHORT_SIGN_MASK
        } else {
            NUMERIC_SHORT
        }) | (dscale << NUMERIC_SHORT_DSCALE_SHIFT)
            | (if weight < 0 {
                NUMERIC_SHORT_WEIGHT_SIGN_MASK
            } else {
                0
            })
            | ((weight as u16) & NUMERIC_SHORT_WEIGHT_MASK);
        bytes.extend_from_slice(&short_header.to_ne_bytes());
    } else {
        bytes.extend_from_slice(&(sign | (dscale & NUMERIC_DSCALE_MASK)).to_ne_bytes());
        bytes.extend_from_slice(&weight.to_ne_bytes());
    }

    for digit in digits {
        bytes.extend_from_slice(&digit.to_ne_bytes());
    }

    bytes
}

fn encode_varlena_header(len: usize) -> [u8; 4] {
    #[cfg(target_endian = "little")]
    {
        ((len as u32) << 2).to_ne_bytes()
    }

    #[cfg(target_endian = "big")]
    {
        ((len as u32) & 0x3FFF_FFFF).to_ne_bytes()
    }
}

fn set_varlena_header(bytes: &mut [u8]) {
    let len = bytes.len();
    bytes[..VARHDRSZ].copy_from_slice(&encode_varlena_header(len));
}
