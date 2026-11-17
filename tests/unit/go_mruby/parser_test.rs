//! upstream file: vendorlib/go-mruby/parser_test.go

use cabby_testkit::reference::mruby::*;

/// upstream: vendorlib/go-mruby/parser_test.go#TestParserGenerateCode
#[test]
fn test_parser_generate_code() {
    let mrb = Mrb::new();

    let mut p = Parser::new(&mrb);

    // Go: `warns != nil` fails; Go never returns a non-nil empty slice, so no
    // warnings is an empty Vec here.
    let warns = p.parse(r#""foo""#, None).unwrap_or_else(|e| panic!("err: {}", e.error()));
    assert!(warns.is_empty(), "warnings: {warns:?}");

    let proc = p.generate_code();
    let result = mrb.run(&proc.into(), None).unwrap_or_else(|e| panic!("err: {}", e.error()));
    assert_eq!(result.string(), "foo", "bad");

    p.close();
    mrb.close();
}

/// upstream: vendorlib/go-mruby/parser_test.go#TestParserParse
#[test]
fn test_parser_parse() {
    let mrb = Mrb::new();

    let mut p = Parser::new(&mrb);

    let warns = p.parse(r#""foo""#, None).unwrap_or_else(|e| panic!("err: {}", e.error()));
    assert!(warns.is_empty(), "warnings: {warns:?}");

    p.close();
    mrb.close();
}

/// upstream: vendorlib/go-mruby/parser_test.go#TestParserParse_error
#[test]
fn test_parser_parse_error() {
    let mrb = Mrb::new();

    let mut p = Parser::new(&mrb);

    assert!(p.parse("def foo", None).is_err(), "should have errors");

    p.close();
    mrb.close();
}

/// upstream: vendorlib/go-mruby/parser_test.go#TestParserError_error
/// note: runtime adaptation (compile-time interface check -> trait + rendering): `var _ error = new(ParserError)` becomes: the zero ParserError is a std::error::Error and renders like Go Error() of a zero value ("Ruby parse error!\n\n").
///
/// Runtime adaptation (Go interface satisfaction -> trait + usable error):
/// upstream only checks at compile time that `*ParserError` implements `error`
/// (`var _ error = new(ParserError)`). The Rust invariant: the zero parse error
/// implements `std::error::Error` and renders like Go's `Error()` of a zero
/// value (`Ruby parse error!\n\n`), so it is usable wherever an error is.
#[test]
fn test_parser_error_error() {
    let e = parser_error_zero();
    let as_error: &dyn std::error::Error = &e;
    assert_eq!(as_error.to_string(), "Ruby parse error!\n\n");
}
