# frozen_string_literal: true

require 'bunny'
require 'net_http_unix'

http = NetX::HTTPUnix.new("unix://#{ENV['NOMAD_SECRETS_DIR']}/api.sock")
req = Net::HTTP::Get.new(URI('/v1/agent/self'))
resp = http.request(req)
puts 'Connected to agent: #{resp.body}'

rmq = Bunny.new
rmq.start
rmqc = rmq.create_channel
q = rmqc.queue('contact_watcher_entry')
xch = rmqc.exchange('katc', type: 'topic')
q.bind(xch, routing_key: 'contact_started')
puts 'Connected to Rabbit'

begin
  q.subscribe(block: true) do |_info, _properties, body|
    puts "Starting job for #{body}"
    req = Net::HTTP::Post.new(URI('/v1/job/katc-watcher/dispatch'), {}.to_json, initheader = {'Content-Type': 'application/json'})
    resp = http.request(req)
    puts resp.body
  end
end