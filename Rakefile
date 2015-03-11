require 'rubygems'
require 'fileutils'
require 'bundler/gem_tasks'
require 'rspec/core/rake_task'
require 'rake/extensiontask'

Rake::ExtensionTask.new do |t|
  t.name = 'hermann_lib'
  t.ext_dir = 'ext/hermann'
  t.gem_spec = Gem::Specification.load('hermann.gemspec')
end

RSpec::Core::RakeTask.new(:spec) do |r|
  options = ['--tag ~type:integration']

  r.rspec_opts = options.join(' ')
end

namespace :spec do
  RSpec::Core::RakeTask.new(:integration) do |r|
    options = ['--tag type:integration']
    r.rspec_opts = options.join(' ')
  end
end

desc 'Remove the entire ./tmp directory'
task :removetmp do
  FileUtils.rm_rf('tmp')
end

task :clean => [:removetmp]

task :build => [:compile]
task :default => [:clean, :build, :spec]

