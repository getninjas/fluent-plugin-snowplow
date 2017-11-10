require_relative '../helper'
require 'net/http'
require 'base64'
require 'yajl'
require 'fluent/test'
require 'fluent/plugin/out_snowplow'
require 'fluent/plugin/snowplow/version'

class SnowplowOutputTestBase < Test::Unit::TestCase
  def self.port
    5176
  end

  def self.server_config
    config = {BindAddress: '127.0.0.1', Port: port}
    if ENV['VERBOSE']
      logger = WEBrick::Log.new(STDOUT, WEBrick::BasicLog::DEBUG)
      config[:Logger] = logger
      config[:AccessLog] = []
    end
    config
  end

  def self.test_http_client(**opts)
    opts = opts.merge(open_timeout: 1, read_timeout: 1)
    Net::HTTP.start('127.0.0.1', port, **opts)
  end

  # setup / teardown for servers
  def setup
    Fluent::Test.setup
    @gets = []
    @posts = []
    @prohibited = 0
    @requests = 0
    @dummy_server_thread = Thread.new do
      srv = WEBrick::HTTPServer.new(self.class.server_config)
      begin
        allowed_methods = %w(POST)
        srv.mount_proc('/com.snowplowanalytics.snowplow/') { |req,res|
          @requests += 1
          unless allowed_methods.include? req.request_method
            res.status = 405
            res.body = 'request method mismatch'
            next
          end

          record = {}
          if req.content_type.start_with? 'application/json'
            record[:json] = Yajl.load(req.body)
          end

          instance_variable_get("@#{req.request_method.downcase}s").push(record)

          res.status = 200
        }
        allowed_methods_i = %w(GET)
        srv.mount_proc('/i') { |req,res|
          @requests += 1
          unless allowed_methods_i.include? req.request_method
            res.status = 405
            res.body = 'request method mismatch'
            next
          end

          record = {}
          record[:query] = req.query()

          instance_variable_get("@#{req.request_method.downcase}s").push(record)

          res.status = 200
        }
        srv.mount_proc('/') { |req,res|
          res.status = 200
          res.body = 'running'
        }
        srv.start
      ensure
        srv.shutdown
      end
    end

    # to wait completion of dummy server.start()
    require 'thread'
    cv = ConditionVariable.new
    watcher = Thread.new {
      connected = false
      while not connected
        begin
          client = self.class.test_http_client
          client.request_get('/')
          connected = true
        rescue Errno::ECONNREFUSED
          sleep 0.1
        rescue StandardError => e
          p e
          sleep 0.1
        end
      end
      cv.signal
    }
    mutex = Mutex.new
    mutex.synchronize {
      cv.wait(mutex)
    }
  end

  def test_dummy_server
    client = self.class.test_http_client
    post_header = { 'Content-Type' => 'application/json' }

    assert_equal '200', client.request_get('/').code
    assert_equal '200', client.request_post('/com.snowplowanalytics.snowplow/', Yajl::Encoder.encode({'hello' => 'world'}), post_header).code

    assert_equal 1, @posts.size

    assert_equal 'world', @posts[0][:json]['hello']

    assert_equal '200', client.request_get('/i?hello=world').code

    assert_equal 1, @gets.size

    assert_equal 'world', @gets[0][:query]['hello']
  end

  def teardown
    @dummy_server_thread.kill
    @dummy_server_thread.join
  end

  def create_driver(conf, tag='test.metrics')
    Fluent::Test::BufferedOutputTestDriver.new(Fluent::SnowplowOutput, tag).configure(conf)
  end
end
#
# class HTTPOutputTest < HTTPOutputTestBase
#   CONFIG = %[
#     endpoint_url http://127.0.0.1:#{port}/api/
#   ]
#
#   CONFIG_JSON = %[
#     endpoint_url http://127.0.0.1:#{port}/api/
#     serializer json
#   ]
#
#   CONFIG_PUT = %[
#     endpoint_url http://127.0.0.1:#{port}/api/
#     http_method put
#   ]
#
#   CONFIG_HTTP_ERROR = %[
#     endpoint_url https://127.0.0.1:#{port - 1}/api/
#   ]
#
#   CONFIG_HTTP_ERROR_SUPPRESSED = %[
#     endpoint_url https://127.0.0.1:#{port - 1}/api/
#     raise_on_error false
#   ]
#
#   RATE_LIMIT_MSEC = 1200
#
#   CONFIG_RATE_LIMIT = %[
#     endpoint_url http://127.0.0.1:#{port}/api/
#     rate_limit_msec #{RATE_LIMIT_MSEC}
#   ]
#
#   def test_configure
#     d = create_driver CONFIG
#     assert_equal "http://127.0.0.1:#{self.class.port}/api/", d.instance.endpoint_url
#     assert_equal :form, d.instance.serializer
#
#     d = create_driver CONFIG_JSON
#     assert_equal "http://127.0.0.1:#{self.class.port}/api/", d.instance.endpoint_url
#     assert_equal :json, d.instance.serializer
#   end
#
#   def test_emit_form
#     d = create_driver CONFIG
#     d.emit({ 'field1' => 50, 'field2' => 20, 'field3' => 10, 'otherfield' => 1, 'binary' => "\xe3\x81\x82".force_encoding("ascii-8bit") })
#     d.run
#
#     assert_equal 1, @posts.size
#     record = @posts[0]
#
#     assert_equal '50', record[:form]['field1']
#     assert_equal '20', record[:form]['field2']
#     assert_equal '10', record[:form]['field3']
#     assert_equal '1', record[:form]['otherfield']
#     assert_equal URI.encode_www_form_component("あ").upcase, record[:form]['binary'].upcase
#     assert_nil record[:auth]
#
#     d.emit({ 'field1' => 50, 'field2' => 20, 'field3' => 10, 'otherfield' => 1 })
#     d.run
#
#     assert_equal 2, @posts.size
#   end
#
#   def test_emit_form_put
#     d = create_driver CONFIG_PUT
#     d.emit({ 'field1' => 50 })
#     d.run
#
#     assert_equal 0, @posts.size
#     assert_equal 1, @puts.size
#     record = @puts[0]
#
#     assert_equal '50', record[:form]['field1']
#     assert_nil record[:auth]
#
#     d.emit({ 'field1' => 50 })
#     d.run
#
#     assert_equal 0, @posts.size
#     assert_equal 2, @puts.size
#   end
#
#   def test_emit_json
#     binary_string = "\xe3\x81\x82"
#     d = create_driver CONFIG_JSON
#     d.emit({ 'field1' => 50, 'field2' => 20, 'field3' => 10, 'otherfield' => 1, 'binary' => binary_string })
#     d.run
#
#     assert_equal 1, @posts.size
#     record = @posts[0]
#
#     assert_equal 50, record[:json]['field1']
#     assert_equal 20, record[:json]['field2']
#     assert_equal 10, record[:json]['field3']
#     assert_equal 1, record[:json]['otherfield']
#     assert_equal binary_string, record[:json]['binary']
#     assert_nil record[:auth]
#   end
#
#   def test_http_error_is_raised
#     d = create_driver CONFIG_HTTP_ERROR
#     assert_raise Errno::ECONNREFUSED do
#       d.emit({ 'field1' => 50 })
#     end
#   end
#
#   def test_http_error_is_suppressed_with_raise_on_error_false
#     d = create_driver CONFIG_HTTP_ERROR_SUPPRESSED
#     d.emit({ 'field1' => 50 })
#     d.run
#     # drive asserts the next output chain is called;
#     # so no exception means our plugin handled the error
#
#     assert_equal 0, @requests
#   end
#
#   def test_rate_limiting
#     d = create_driver CONFIG_RATE_LIMIT
#     record = { :k => 1 }
#
#     last_emit = _current_msec
#     d.emit(record)
#     d.run
#
#     assert_equal 1, @posts.size
#
#     d.emit({})
#     d.run
#     assert last_emit + RATE_LIMIT_MSEC > _current_msec, "Still under rate limiting interval"
#     assert_equal 1, @posts.size
#
#     wait_msec = 500
#     sleep (last_emit + RATE_LIMIT_MSEC - _current_msec + wait_msec) * 0.001
#
#     assert last_emit + RATE_LIMIT_MSEC < _current_msec, "No longer under rate limiting interval"
#     d.emit(record)
#     d.run
#     assert_equal 2, @posts.size
#   end
#
#   def _current_msec
#     Time.now.to_f * 1000
#   end
#
#   def test_auth
#     @auth = true # enable authentication of dummy server
#
#     d = create_driver(CONFIG, 'test.metrics')
#     d.emit({ 'field1' => 50, 'field2' => 20, 'field3' => 10, 'otherfield' => 1 })
#     d.run # failed in background, and output warn log
#
#     assert_equal 0, @posts.size
#     assert_equal 1, @prohibited
#
#     d = create_driver(CONFIG + %[
#       authentication basic
#       username alice
#       password wrong_password
#     ], 'test.metrics')
#     d.emit({ 'field1' => 50, 'field2' => 20, 'field3' => 10, 'otherfield' => 1 })
#     d.run # failed in background, and output warn log
#
#     assert_equal 0, @posts.size
#     assert_equal 2, @prohibited
#
#     d = create_driver(CONFIG + %[
#       authentication basic
#       username alice
#       password secret!
#     ], 'test.metrics')
#     d.emit({ 'field1' => 50, 'field2' => 20, 'field3' => 10, 'otherfield' => 1 })
#     d.run # failed in background, and output warn log
#
#     assert_equal 1, @posts.size
#     assert_equal 2, @prohibited
#   end
#
# end

class SnowplowOutputTest < SnowplowOutputTestBase

  def test_that_it_has_a_version_number
    refute_nil ::Fluent::Snowplow::VERSION
  end

  CONFIG_GET = %[
    # Snowplow Emitter Config
    host          127.0.0.1
    port          #{port}
    buffer_size   0
    protocol      http
    method        get

    # Buffered Output Config
    buffer_type   memory
  ]

  CONFIG_POST = %[
    # Snowplow Emitter Config
    host          127.0.0.1
    port          #{port}
    buffer_size   10
    protocol      http
    method        post

    # Buffered Output Config
    buffer_type   memory
  ]

  def test_configure
    d = create_driver CONFIG_POST
    i = d.instance
    assert_equal '127.0.0.1', i.host
    assert_equal 10, i.buffer_size
    assert_equal 'http', i.protocol
    assert_equal 'post', i.method
    assert_equal self.class.port, i.port
  end

  def test_emit_get
    schema = 'iglu:com.my_company/movie_poster/jsonschema/1-0-0'
    aid = 'app1'
    true_tstamp = Date.new(2017, 11, 1).strftime("%Q")

    d = create_driver CONFIG_GET
    message = {
        'movie_name' => 'Solaris',
        'poster_country' => 'JP',
        'poster_year$dt' => Date.new(1978, 1, 1).iso8601
    }
    d.emit({'application' => aid,
            'schema' => schema,
            'true_timestamp' => true_tstamp,
            'message' => Yajl::Encoder.encode(message)})
    d.run

    assert_equal 1, @gets.size
    event = @gets[0][:query]

    assert_equal 'ue', event['e']
    assert_equal aid, event['aid']
    assert_equal true_tstamp, event['ttm']

    payload = Yajl::Parser.parse( Base64.strict_decode64(event['ue_px']))['data']
    assert_equal schema, payload['schema']
    assert_equal message, payload['data']
  end

  def test_emit_post
    schema = 'iglu:com.my_company/movie_poster/jsonschema/1-0-0'
    aid = 'app1'
    true_tstamp = Date.new(2017, 11, 1).strftime("%Q")

    d = create_driver CONFIG_POST
    message = {
        'movie_name' => 'Solaris',
        'poster_country' => 'JP',
        'poster_year$dt' => Date.new(1978, 1, 1).iso8601
    }
    d.emit({'application' => aid,
            'schema' => schema,
            'true_timestamp' => true_tstamp,
            'message' => Yajl::Encoder.encode(message)})
    d.run

    assert_equal 1, @posts.size
    record = @posts[0]

    assert_equal 'iglu:com.snowplowanalytics.snowplow/payload_data/jsonschema/1-0-4', record[:json]['schema']

    event = record[:json]['data'][0]
    assert_equal 'ue', event['e']
    assert_equal aid, event['aid']
    assert_equal true_tstamp, event['ttm']

    payload = Yajl::Parser.parse( Base64.strict_decode64(event['ue_px']))['data']
    assert_equal schema, payload['schema']
    assert_equal message, payload['data']
  end

end
