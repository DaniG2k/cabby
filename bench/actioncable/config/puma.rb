# frozen_string_literal: true

# Puma for the Action Cable benchmark server; see README.md. `sut` sets the environment.
#
# Single mode (no cluster workers): Action Cable's async adapter is in-process, so one process
# holds every connection, as the Go and Rust servers do. WebSocket connections are hijacked from
# Puma right after the handshake and then run on Action Cable's own event loop and worker pool;
# Puma's threads only serve handshakes and broadcast POSTs. 5 threads is Puma's default maximum.
environment ENV.fetch("RAILS_ENV", "production")
workers 0
threads_count = Integer(ENV.fetch("PUMA_THREADS", "5"))
threads threads_count, threads_count

host = ENV.fetch("HOST", "127.0.0.1")
ENV.fetch("PORTS", "8080").split(",").uniq.each { |port| bind "tcp://#{host}:#{port}" }

rackup File.expand_path("../config.ru", __dir__)
log_requests false
quiet
# The harness stops the server with SIGTERM and kills it 5 s later; open WebSockets would
# otherwise hold Puma's graceful shutdown.
force_shutdown_after 1
