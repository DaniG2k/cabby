# frozen_string_literal: true

require_relative "app"

map(ENV.fetch("BROADCAST_PATH", "/broadcast")) { run BroadcastEndpoint }
run Rails.application
