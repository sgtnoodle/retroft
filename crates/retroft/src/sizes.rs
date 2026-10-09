/// Parse explicit byte units with exact decimal arithmetic, never floating point.
pub fn parse_size(value: &str) -> Result<usize, String> {
    let value = value.trim();
    let split = value
        .find(|character: char| !character.is_ascii_digit() && character != '.')
        .unwrap_or(value.len());
    let (number, unit) = value.split_at(split);
    let multiplier: u128 = match unit.trim() {
        "B" => 1,
        "KiB" => 1 << 10,
        "MiB" => 1 << 20,
        "GiB" => 1 << 30,
        "TiB" => 1 << 40,
        "kB" | "KB" => 1_000,
        "MB" => 1_000_000,
        "GB" => 1_000_000_000,
        "TB" => 1_000_000_000_000,
        _ => {
            return Err("size requires a unit: B, KiB, MiB, GiB, TiB, kB/KB, MB, GB, or TB".into())
        }
    };
    let (whole, fraction) = number.split_once('.').unwrap_or((number, ""));
    // The prefix contains only ASCII digits and dots; reject extra dots here.
    if whole.is_empty() || fraction.contains('.') || number.ends_with('.') {
        return Err("size requires a positive decimal number before its unit".into());
    }
    let fraction = fraction.trim_end_matches('0');
    let whole = whole.parse::<u128>().map_err(|_| "size overflows")?;
    let numerator = if fraction.is_empty() {
        0
    } else {
        fraction
            .parse::<u128>()
            .map_err(|_| "size precision overflows")?
    };
    let exponent = fraction
        .len()
        .try_into()
        .map_err(|_| "size precision overflows")?;
    let denominator = 10u128
        .checked_pow(exponent)
        .ok_or("size precision overflows")?;
    let (mut divisor, mut remainder) = (multiplier, denominator);
    while remainder != 0 {
        (divisor, remainder) = (remainder, divisor % remainder);
    }
    let denominator = denominator / divisor;
    if numerator % denominator != 0 {
        return Err("size must resolve to an exact whole number of bytes".into());
    }
    let fractional_bytes = (numerator / denominator)
        .checked_mul(multiplier / divisor)
        .ok_or("size overflows")?;
    let bytes = whole
        .checked_mul(multiplier)
        .and_then(|bytes| bytes.checked_add(fractional_bytes))
        .ok_or("size overflows")?;
    let bytes = usize::try_from(bytes).map_err(|_| "size exceeds this platform's address range")?;
    if bytes == 0 {
        return Err("size must be positive".into());
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn units_are_explicit_and_fractions_are_exact() {
        for (input, expected) in [
            ("1B", 1),
            ("1KiB", 1024),
            ("1MiB", 1_048_576),
            ("1GiB", 1_073_741_824),
            ("1kB", 1000),
            ("1KB", 1000),
            ("1MB", 1_000_000),
            ("1GB", 1_000_000_000),
            ("1.5MiB", 1_572_864),
            ("0.125KiB", 128),
            ("0.001kB", 1),
            ("0.00000095367431640625MiB", 1),
            ("1.000000000000000000MiB", 1_048_576),
            (" 2 KiB ", 2048),
        ] {
            assert_eq!(parse_size(input).unwrap(), expected, "{input}");
        }
        assert_eq!(parse_size(&format!("{}B", usize::MAX)).unwrap(), usize::MAX);
        assert!(parse_size(&format!("{}B", usize::MAX as u128 + 1)).is_err());
        if usize::BITS >= 64 {
            assert_eq!(parse_size("1TiB").unwrap() as u128, 1u128 << 40);
            assert_eq!(parse_size("1TB").unwrap() as u128, 1_000_000_000_000);
        } else {
            assert!(parse_size("1TiB").is_err());
        }
    }

    #[test]
    fn rejects_unitless_nonpositive_malformed_and_overflow_values() {
        assert!(parse_size(&format!("1.{}1MiB", "0".repeat(4096))).is_err());
        for input in [
            "",
            "1024",
            "0B",
            "0.0MiB",
            "-1MiB",
            "+1MiB",
            "NaNMiB",
            "infMiB",
            "1kb",
            "1Mi",
            "1e3B",
            "1..5MiB",
            ".5MiB",
            "1.MiB",
            "0.1B",
            "0.1KiB",
            "340282366920938463463374607431768211456B",
            "340282366920938463463374607431768211455TiB",
        ] {
            assert!(parse_size(input).is_err(), "accepted {input}");
        }
    }
}
