use mini_aurora_common::{PageId, PAGE_SIZE};

pub fn parse_page_id(value: &str) -> Result<PageId, &'static str> {
    value.parse().map_err(|_| "Invalid page_id")
}

pub fn parse_offset(value: &str) -> Result<u16, &'static str> {
    value.parse().map_err(|_| "Invalid offset")
}

pub fn parse_len(value: &str) -> Result<usize, &'static str> {
    value.parse().map_err(|_| "Invalid len")
}

pub fn ensure_range_in_page(offset: u16, len: usize) -> Result<(), String> {
    let offset_usize = usize::from(offset);
    if offset_usize + len > PAGE_SIZE {
        return Err(format!(
            "Range out of bounds: offset {offset_usize} + len {len} exceeds PAGE_SIZE {PAGE_SIZE}"
        ));
    }
    Ok(())
}

pub fn print_page_text(page: &[u8]) {
    let end = page.iter().position(|&b| b == 0).unwrap_or(PAGE_SIZE);
    if end == 0 {
        println!("(empty page)");
    } else {
        println!("{:?}", String::from_utf8_lossy(&page[..end]));
    }
}

pub fn print_page_raw(page: &[u8]) {
    for (row, chunk) in page.chunks(16).enumerate() {
        let offset = row * 16;
        print!("{offset:04x}: ");
        for b in chunk {
            print!("{b:02x} ");
        }
        for _ in chunk.len()..16 {
            print!("   ");
        }
        print!(" |");
        for b in chunk {
            let c = if b.is_ascii_graphic() || *b == b' ' {
                *b as char
            } else {
                '.'
            };
            print!("{c}");
        }
        println!("|");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_helpers_validate_input() {
        assert_eq!(parse_page_id("42").unwrap(), 42);
        assert!(parse_page_id("x").is_err());

        assert_eq!(parse_offset("12").unwrap(), 12);
        assert!(parse_offset("-1").is_err());

        assert_eq!(parse_len("8").unwrap(), 8);
        assert!(parse_len("nope").is_err());
    }

    #[test]
    fn range_validation_rejects_overflow() {
        assert!(ensure_range_in_page(0, PAGE_SIZE).is_ok());
        assert!(ensure_range_in_page(1, PAGE_SIZE).is_err());
    }
}
