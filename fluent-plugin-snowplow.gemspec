# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'fluent/plugin/snowplow/version'

Gem::Specification.new do |spec|
  spec.name          = "fluent-plugin-snowplow"
  spec.version       = Fluent::Snowplow::VERSION
  spec.authors       = ["Lucas Souza"]
  spec.email         = ["lucas@getninjas.com.br"]

  spec.summary       = "Fluentd snowplow gem"
  spec.description   = "Fluentd snowplow gem"
  spec.homepage      = "http://www.getninjas.com.br"
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.required_ruby_version = Gem::Requirement.new(">= 2.1.0".freeze)

  spec.add_runtime_dependency "fluentd", [">= 0.12.0", "< 0.14.0"]
  spec.add_runtime_dependency "snowplow-tracker", "~> 0.6.1"

  spec.add_development_dependency "yajl-ruby", "~> 1.0"
  spec.add_development_dependency "bundler", "~> 1.9"
  spec.add_development_dependency "rake", "~> 10.0"
  spec.add_development_dependency "test-unit", "~> 3.1.0"

end
