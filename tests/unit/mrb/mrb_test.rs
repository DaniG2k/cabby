//! upstream file: mrb/mrb_test.go

use cabby_testkit::reference::mrb::Engine;

/// upstream: mrb/mrb_test.go#TestLoadString
#[test]
fn test_load_string() {
    let engine = Engine::new();

    // Upstream ignores LoadString's error.
    let _ = engine.load_string(
        r#"
		module Example
			def self.add(a, b)
				a + b
			end
		end
		"#,
    );

    let result = engine.eval("Example.add(20, 22)");

    // assert.Nil(t, err)
    assert!(result.is_ok(), "err: {:?}", result.as_ref().err().map(|e| e.error()));
    assert_eq!(42, result.expect("checked above").fixnum());
}
