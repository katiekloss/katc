require 'socket'
require 'adsb'

s = TCPSocket.new ENV['DUMP1090_HOST'], 30002

while line = s.gets
  begin
    msg = ADSB::Message.new(line[1..-1])
    puts msg.address
  rescue NameError
    puts "Unknown type code in #{line}"
  end
end

s.close
