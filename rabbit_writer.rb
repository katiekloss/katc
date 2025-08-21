# frozen_string_literal: true

require 'bunny'
require 'socket'

connection = Bunny.new
s = TCPSocket.new(ENV['DUMP1090_HOSTNAME'], 30002)

begin
  connection.start
  channel = connection.create_channel
  xch = channel.exchange('mode_s')
  while (line = s.gets)
    line = line[1..-3]
    xch.publish(line)
  end
rescue
  connection.close
end
