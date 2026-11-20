# frozen_string_literal: true

# A minimal Rails application with only Action Cable, as cabby-bench's third server under test.
# Settings and the reasons for them are in README.md; `sut` launches it.

require "bundler/setup"
require "json"
require "rails"
require "action_cable/engine"

module CableBench
  class Application < Rails::Application
    config.load_defaults 8.1
    config.root = __dir__
    config.eager_load = true
    config.enable_reloading = false
    config.api_only = true
    config.secret_key_base = "cabby-bench-not-a-secret"

    # Minimal logging: the harness discards server output, and Action Cable logs every connection,
    # subscription and (at debug level) every broadcast and transmission. `sut` maps the harness's
    # --log_level to RAILS_LOG_LEVEL.
    config.logger = ActiveSupport::Logger.new($stdout)
    config.log_level = ENV.fetch("RAILS_LOG_LEVEL", "warn").to_sym
    config.action_cable.log_tags = []

    # Single process, so the in-process async adapter (no Redis); the harness's clients send no
    # Origin header, so origin checks are off.
    config.action_cable.cable = { "adapter" => "async" }
    config.action_cable.mount_path = ENV.fetch("CABLE_PATH", "/cable")
    config.action_cable.disable_request_forgery_protection = true
    # The benchmark keeps Action Cable's default worker pool (4 threads); the variable is only
    # for experiments.
    if (size = ENV["ACTIONCABLE_WORKER_POOL_SIZE"])
      config.action_cable.worker_pool_size = Integer(size)
    end
  end
end

# Emulates the latency of cabby-bench's fake RPC application in the callbacks Action Cable runs
# in-process, where AnyCable makes an RPC call: connect, each subscribe command, and disconnect.
# The harness exports these variables only in RPC mode (reconnect_floods); latency is
# `ms × (1 ± jitter)`, uniform, as in the fake RPC server. `sleep` releases the GVL, like I/O.
module AppLatency
  JITTER = Float(ENV.fetch("CABBY_BENCH_APP_LATENCY_JITTER", "0"))
  DELAYS = %w[connect command disconnect].to_h do |kind|
    [kind.to_sym, Float(ENV.fetch("CABBY_BENCH_APP_LATENCY_#{kind.upcase}_MS", "0")) / 1000.0]
  end.freeze

  def self.wait(kind)
    delay = DELAYS.fetch(kind)
    return if delay <= 0

    delay *= 1 + JITTER * (2 * rand - 1) if JITTER.positive?
    sleep delay
  end
end

module ApplicationCable
  # No authentication (the harness runs AnyCable with --noauth, or a fake RPC app that accepts
  # every connection).
  class Connection < ActionCable::Connection::Base
    def connect
      AppLatency.wait(:connect)
    end

    def disconnect
      AppLatency.wait(:disconnect)
    end
  end
end

# AnyCable's public streams: `{"channel":"$pubsub","stream_name":"<name>"}` streams from <name>.
class PubSubChannel < ActionCable::Channel::Base
  def subscribed
    stream_from params[:stream_name]
  end
end

# reconnect_floods' channel; the fake RPC app confirms it and streams from `bench:<identifier>`.
class BenchChannel < ActionCable::Channel::Base
  def subscribed
    AppLatency.wait(:command)
    stream_from "bench:#{identifier}"
  end
end

# "$pubsub" is not a constant name, so the subscription lookup maps it to PubSubChannel. This is
# Subscriptions#add from Action Cable 8.1 with only the class lookup changed.
module PubSubChannelLookup
  CHANNELS = { "$pubsub" => "PubSubChannel" }.freeze

  def add(data)
    id_key = data["identifier"]
    id_options = ActiveSupport::JSON.decode(id_key).with_indifferent_access

    return if subscriptions.key?(id_key)

    subscription_klass = CHANNELS.fetch(id_options[:channel], id_options[:channel]).safe_constantize

    if subscription_klass && ActionCable::Channel::Base > subscription_klass
      subscription = subscription_klass.new(connection, id_key, id_options)
      subscriptions[id_key] = subscription
      subscription.subscribe_to_channel
    else
      logger.error "Subscription class not found: #{id_options[:channel].inspect}"
    end
  end
end
ActionCable::Connection::Subscriptions.prepend(PubSubChannelLookup)

# AnyCable's HTTP broadcast endpoint: a JSON object `{"stream":..,"data":..}` or an array of them.
# Like AnyCable, `data` that is valid JSON is delivered as that JSON value, anything else as a
# string, so clients receive the same `message`. Mounted in config.ru outside the Rails
# middleware stack: in a real app the broadcast call would run inside application code.
module BroadcastEndpoint
  CREATED = [201, {}.freeze, [].freeze].freeze

  def self.call(env)
    return [405, {}, []] unless env["REQUEST_METHOD"] == "POST"

    payload = JSON.parse(env["rack.input"].read)
    (payload.is_a?(Array) ? payload : [payload]).each do |msg|
      ActionCable.server.broadcast(msg.fetch("stream"), decode(msg.fetch("data")))
    end
    CREATED
  rescue JSON::ParserError, KeyError, TypeError, NoMethodError
    [422, {}, []]
  end

  def self.decode(data)
    return data unless data.is_a?(String)

    JSON.parse(data)
  rescue JSON::ParserError
    data
  end
end

Rails.application.initialize!
