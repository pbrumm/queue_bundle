# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)
require "queue_bundle/version"

Gem::Specification.new do |s|
  s.name        = "queue_bundle"
  s.version     = QueueBundle::VERSION
  s.authors     = ["Pete Brumm"]
  s.email       = ["pbrumm@edgenet.com"]
  s.homepage    = ""
  s.summary     = %q{Provides a way to split queue put's into multiple seperate queue pulls}
  s.description = %q{Allows queue work to be distributed to seperate threads that need a consistent end point.   Allows you to provide a hashing algorithm for which thread handles the work.}

  s.rubyforge_project = "queue_bundle"

  s.files         = Dir['bin/*'] + Dir['lib/*'] + Dir['lib/**/*.rb'] + Dir['test/**/*.rb']
  s.test_files    = Dir['test/**/*.rb']
 # s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib"]
end
