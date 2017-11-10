require 'snowplow-tracker'

class Fluent::SnowplowOutput < Fluent::TimeSlicedOutput
  Fluent::Plugin.register_output('snowplow', self)

  config_param :host, :string
  config_param :port, :integer
  config_param :buffer_size, :integer
  config_param :protocol, :string
  config_param :method, :string

  def configure(conf)
    super
  end

  def start
    super

    # see https://github.com/snowplow/snowplow/wiki/Ruby-Tracker#5-emitters
    @emitter = SnowplowTracker::Emitter.new(@host, {
      buffer_size: @buffer_size,
      protocol: @protocol,
      port: @port,
      method: @method,
      on_success: ->(_) { log.debug("Flush with success on snowplow") },
      on_failure: ->(_, _) { raise "Error when flushing to snowplow" }
    })

    @trackers = {}
  end

  def stop
    @tracker.flush
  end

  def format(tag, time, record)
    [tag, time, record].to_msgpack
  end

  def tracker_for(application)
    @trackers[application] ||= SnowplowTracker::Tracker.new(@emitter, nil, nil, application)
    @trackers[application]
  end

  def write(chunk)
    application, tracker = nil, nil

    chunk.msgpack_each do |_, _, record|
      schema = record['schema']
      message = JSON.parse record['message']
      true_timestamp = record['true_timestamp']
      application = record['application']
      tracker = tracker_for(application)

      self_describing_json = SnowplowTracker::SelfDescribingJson.new(schema, message)
      tracker.track_self_describing_event(self_describing_json, nil, SnowplowTracker::TrueTimestamp.new(true_timestamp.to_i))
    end

    tracker.flush
  end
end
