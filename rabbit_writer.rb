# frozen_string_literal: true

require 'bunny'
require 'socket'

connection = Bunny.new
s = TCPSocket.new(ENV['DUMP1090_HOSTNAME'], 30002)

connection.start
channel = connection.create_channel
xch = channel.topic('katc')

while (line = s.gets)
  line = line[1..-3]
  xch.publish(line, routing_key: "mode_s")
end