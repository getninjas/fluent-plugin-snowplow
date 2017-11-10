#!/usr/bin/env rake
require "bundler/gem_tasks"

require "rake/testtask"

Rake::TestTask.new(:test) do |t|
  # To run test for only one file (or file path pattern)
  #  $ bundle exec rake base_test TEST=test/test_specified_path.rb
  #  $ bundle exec rake base_test TEST=test/test_*.rb
  t.libs << "test"
  t.test_files = Dir["test/**/test_*.rb"].sort
  t.verbose = true
end

task :default => :test
