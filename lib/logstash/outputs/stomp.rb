# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"

class LogStash::Outputs::Stomp < LogStash::Outputs::Base
  config_name "stomp"

  config :hosts, :validate => :string, :required => true
  config :user, :validate => :string, :default => ""
  config :password, :validate => :password, :default => ""
  config :destination, :validate => :string, :required => true
  config :vhost, :validate => :string, :default => nil
  config :headers, :validate => :hash
  config :debug, :validate => :boolean, :default => false
  config :stomp_connection_timeout, :validate => :number, :default => 30
  config :stomp_protocol, :validate => ["stomp", "stomp+ssl"], :default => "stomp"


  def register
    require "onstomp"
    @hosts = @hosts.split(",").map { |host| parse_host(host) }
    raise LogStash::ConfigurationError, "At least two STOMP server hosts must be specified in the 'hosts' configuration option for failover." if @hosts.length < 2


    # Introduced a thread-safe event queue (@event_queue) to buffer events during the reconnection phase. This helps prevent potential data loss during the brief period when the connection is reestablished.
    
    @event_queue = SizedQueue.new(10000)
    connect
  end

  def handle_connection_closed
    @logger.warn("STOMP connection closed. Reconnecting...")
    reconnect
  end

  def reconnect
    if @client&.connected?
      @logger.debug("Disconnecting from the current STOMP server")
      @client.disconnect
    end

    # Switch to the next host in the array for failover
    @hosts.rotate!
    connect
  end

  def parse_host(host)
    host, port = host.split(":")
    { "host" => host, "port" => port || 61613 }
  end

  def connect
    begin
      host = @hosts.first
      host_uri = "#{@stomp_protocol}://#{host["host"]}:#{host["port"]}"
      @logger.info("Attempting to connect to STOMP server #{host_uri} with timeout #{stomp_connection_timeout}")

      ssl_options = { :verify_mode => OpenSSL::SSL::VERIFY_NONE }  # Disable hostname verification
      
      Timeout.timeout(@stomp_connection_timeout) do
        @client = OnStomp::Client.new(host_uri, :login => @user, :passcode => @password.value, :ssl => ssl_options)
        @client.on_connection_closed { handle_connection_closed }
        @client.connect
        @logger.debug("Connected to STOMP server") if @client.connected?
      end

      # Start a separate thread to handle event processing
      start_event_processing_thread
    rescue Timeout::Error => e
      log_connection_error("Connection attempt to STOMP server timed out for host #{host['host']}. Will retry.", e)
      reconnect
    rescue Errno::ECONNREFUSED => e
      log_connection_error("Connection to STOMP server refused for host #{host['host']}. Switching to the next host for failover.", e)
      reconnect
    rescue SocketError, Errno::ENETUNREACH => e
      log_network_error("Network-related error while connecting to STOMP server for host #{host['host']}. Will retry.", e)
      sleep 2
      retry
    rescue => e
      log_connection_error("Failed to connect to STOMP server for host #{host['host']}, will retry", e)
      sleep 2
      retry
    end
  end

  def start_event_processing_thread
    Thread.new do
      loop do
        process_event(@event_queue.pop)
      end
    end
  end

  def log_network_error(message, exception)
    @logger.error(message, :exception => exception, :backtrace => exception.backtrace)
  end

  def log_connection_error(message, exception)
    @logger.debug(message, :exception => exception, :backtrace => exception.backtrace)
  end

  def close
    @logger.warn("Disconnecting from STOMP broker")
    @client.disconnect if @client&.connected?
  end

  def multi_receive(events)
    @logger.debug("STOMP sending events in batch", { :host => @hosts.first["host"], :events => events.length })

    events.each do |event|
      # Enqueue the event for processing
      @event_queue.push(event)
    end
  end

  def process_event(event)
    return unless event

    headers = build_headers(event)
    
    # Use a transaction for sending the event
    @client.transaction do |t|
      t.send(event.sprintf(@destination), event.to_json, headers)
    end
  end

  def build_headers(event)
    return unless @headers

    @headers.transform_values { |v| event.sprintf(v) }
  end
end
