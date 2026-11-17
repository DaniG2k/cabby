//! upstream file: api/auth_test.go

use cabby_testkit::reference::api::*;

mod test_authenticator {
    use super::*;

    /// upstream: api/auth_test.go#TestAuthenticator/NewAuthenticator_with_explicit_secret
    #[test]
    fn new_authenticator_with_explicit_secret() {
        let auth = new_authenticator("my-secret").expect("authenticator");

        assert!(auth.is_enabled());
        assert_eq!("my-secret", auth.secret());
    }

    /// upstream: api/auth_test.go#TestAuthenticator/Authenticate_with_no_secret_configured_(public)
    #[test]
    fn authenticate_with_no_secret_configured_public() {
        let auth = new_authenticator("").expect("authenticator");

        let req = request("POST", "/api/publish", &[], b"");
        assert!(auth.authenticate(&req));
    }

    /// upstream: api/auth_test.go#TestAuthenticator/Authenticate_with_valid_Bearer_token
    #[test]
    fn authenticate_with_valid_bearer_token() {
        let auth = new_authenticator("test-secret").expect("authenticator");

        let req = request("POST", "/api/publish", &[("Authorization", "Bearer test-secret")], b"");

        assert!(auth.authenticate(&req));
    }

    /// upstream: api/auth_test.go#TestAuthenticator/Authenticate_rejects_missing_Authorization_header
    #[test]
    fn authenticate_rejects_missing_authorization_header() {
        let auth = new_authenticator("test-secret").expect("authenticator");

        let req = request("POST", "/api/publish", &[], b"");

        assert!(!auth.authenticate(&req));
    }

    /// upstream: api/auth_test.go#TestAuthenticator/Authenticate_rejects_invalid_token
    #[test]
    fn authenticate_rejects_invalid_token() {
        let auth = new_authenticator("test-secret").expect("authenticator");

        let req = request("POST", "/api/publish", &[("Authorization", "Bearer wrong-secret")], b"");

        assert!(!auth.authenticate(&req));
    }

    /// upstream: api/auth_test.go#TestAuthenticator/Authenticate_rejects_non-Bearer_auth
    #[test]
    fn authenticate_rejects_non_bearer_auth() {
        let auth = new_authenticator("test-secret").expect("authenticator");

        let req = request("POST", "/api/publish", &[("Authorization", "Basic dXNlcjpwYXNz")], b"");

        assert!(!auth.authenticate(&req));
    }
}

/// upstream: api/auth_test.go#TestAuthenticator_Integration
#[test]
fn test_authenticator_integration() {
    let auth = new_authenticator("my-secret").expect("authenticator");

    let req = request("POST", "/api/publish", &[("Authorization", "Bearer my-secret")], b"");

    assert!(auth.authenticate(&req));
}
