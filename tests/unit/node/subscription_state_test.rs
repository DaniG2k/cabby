//! upstream file: node/subscription_state_test.go
//!
//! Channel identifiers are opaque strings: identifiers that differ only in a
//! parameter (`channelId`) are distinct subscriptions, and JSON inside them is
//! never normalized.

use cabby_testkit::reference::node::*;

use super::support::{sorted, strings};

mod test_subscription_state_channels {
    use super::*;

    /// upstream: node/subscription_state_test.go#TestSubscriptionStateChannels/with_different_channels
    /// upstream: node/subscription_state_test.go#TestSubscriptionStateChannels
    #[test]
    fn with_different_channels() {
        let subscriptions = new_subscription_state();

        subscriptions.add_channel("{\"channel\":\"SystemNotificationChannel\"}");
        subscriptions.add_channel("{\"channel\":\"DressageChannel\",\"id\":23376}");

        let expected =
            ["{\"channel\":\"SystemNotificationChannel\"}", "{\"channel\":\"DressageChannel\",\"id\":23376}"];

        let actual = subscriptions.channels();

        for key in expected {
            assert!(actual.iter().any(|c| c == key), "{actual:?} does not contain {key:?}");
        }
    }

    /// upstream: node/subscription_state_test.go#TestSubscriptionStateChannels/with_the_same_channel
    /// upstream: node/subscription_state_test.go#TestSubscriptionStateChannels
    #[test]
    fn with_the_same_channel() {
        let subscriptions = new_subscription_state();

        subscriptions.add_channel("{\"channel\":\"GraphqlChannel\",\"channelId\":\"165d8949069\"}");
        subscriptions.add_channel("{\"channel\":\"GraphqlChannel\",\"channelId\":\"165d8941e62\"}");

        let expected = [
            "{\"channel\":\"GraphqlChannel\",\"channelId\":\"165d8949069\"}",
            "{\"channel\":\"GraphqlChannel\",\"channelId\":\"165d8941e62\"}",
        ];

        let actual = subscriptions.channels();

        for key in expected {
            assert!(actual.iter().any(|c| c == key), "{actual:?} does not contain {key:?}");
        }
    }

    /// Upstream only checks inclusion (`assert.Contains`), which a state that
    /// collapsed both parameterized identifiers into one entry plus a stray
    /// value could still pass. Each identifier is its own subscription, kept
    /// byte for byte, and removing one leaves the other.
    ///
    /// regression-for: node/subscription_state_test.go#TestSubscriptionStateChannels/with_the_same_channel
    #[test]
    fn parameterized_identifiers_are_separate_subscriptions() {
        let a = "{\"channel\":\"GraphqlChannel\",\"channelId\":\"165d8949069\"}";
        let b = "{\"channel\":\"GraphqlChannel\",\"channelId\":\"165d8941e62\"}";
        // Same channel and parameter, different key order: still a distinct identifier.
        let c = "{\"channelId\":\"165d8949069\",\"channel\":\"GraphqlChannel\"}";

        let subscriptions = new_subscription_state();
        subscriptions.add_channel(a);
        subscriptions.add_channel(b);
        subscriptions.add_channel(c);

        assert_eq!(sorted(strings(&[a, b, c])), sorted(subscriptions.channels()));
        assert!(subscriptions.has_channel(a));
        assert!(subscriptions.has_channel(b));
        assert!(subscriptions.has_channel(c));

        subscriptions.add_channel_stream(a, "graphql:a");
        subscriptions.add_channel_stream(b, "graphql:b");

        subscriptions.remove_channel(a);

        assert_eq!(sorted(strings(&[b, c])), sorted(subscriptions.channels()));
        assert!(!subscriptions.has_channel(a));
        assert_eq!(None, subscriptions.streams_for(a));
        assert_eq!(Some(strings(&["graphql:b"])), subscriptions.streams_for(b));
    }
}

/// upstream: node/subscription_state_test.go#TestSubscriptionStreamsFor
#[test]
fn test_subscription_streams_for() {
    let subscriptions = new_subscription_state();

    subscriptions.add_channel("chat_1");
    subscriptions.add_channel("presence_1");

    subscriptions.add_channel_stream("chat_1", "a");
    subscriptions.add_channel_stream("chat_1", "b");
    subscriptions.add_channel_stream("presence_1", "z");

    assert!(subscriptions.streams_for("chat_1").unwrap_or_default().contains(&"a".to_owned()));
    assert!(subscriptions.streams_for("chat_1").unwrap_or_default().contains(&"b".to_owned()));
    assert_eq!(Some(strings(&["z"])), subscriptions.streams_for("presence_1"));

    subscriptions.remove_channel_streams("chat_1");
    // assert.Empty accepts nil and empty slices alike.
    assert!(subscriptions.streams_for("chat_1").unwrap_or_default().is_empty());
    assert_eq!(Some(strings(&["z"])), subscriptions.streams_for("presence_1"));

    subscriptions.add_channel_stream("presence_1", "y");
    subscriptions.remove_channel_stream("presence_1", "z");
    subscriptions.remove_channel_stream("presence_1", "t");
    assert_eq!(Some(strings(&["y"])), subscriptions.streams_for("presence_1"));
}

/// Upstream ignores `RemoveChannelStreams`' result and checks emptiness with
/// `assert.Empty` (nil or empty). Removing a channel's streams returns exactly
/// those streams, keeps the channel subscribed with an empty (non-nil) stream
/// set, leaves other channels untouched, and returns nil for unknown channels;
/// streams cannot be added to unknown channels.
///
/// regression-for: node/subscription_state_test.go#TestSubscriptionStreamsFor
#[test]
fn removes_only_the_intended_streams() {
    let subscriptions = new_subscription_state();

    subscriptions.add_channel("chat_1");
    subscriptions.add_channel("presence_1");
    subscriptions.add_channel_stream("chat_1", "a");
    subscriptions.add_channel_stream("chat_1", "b");
    subscriptions.add_channel_stream("presence_1", "z");
    subscriptions.add_channel_stream("unknown", "u");

    assert_eq!(Some(strings(&["a", "b"])), subscriptions.remove_channel_streams("chat_1").map(sorted));
    assert!(subscriptions.has_channel("chat_1"));
    assert_eq!(Some(Vec::<String>::new()), subscriptions.streams_for("chat_1"));
    assert_eq!(Some(strings(&["z"])), subscriptions.streams_for("presence_1"));

    assert_eq!(None, subscriptions.remove_channel_streams("unknown"));
    assert!(!subscriptions.has_channel("unknown"));
    assert_eq!(None, subscriptions.streams_for("unknown"));
}
