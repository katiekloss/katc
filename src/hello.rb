require 'socket'
require 'adsb'

s = TCPSocket.new ENV['DUMP1090_HOST'], 30002

while line = s.gets
  line = line[1..-3]
  begin
    msg = ADSB::Message.parse(line)
    next unless msg.is_a?(ADSB::Messages::Base)

    human = case
    when msg.is_a?(ADSB::Messages::Identification)
      "ident #{msg.identification}"
    when msg.is_a?(ADSB::Messages::Position)
      "position #{msg.latitude} #{msg.longitude}"
    when msg.is_a?(ADSB::Messages::Velocity)
      "heading #{msg.heading} velocity #{msg.velocity}"
    else
      ""
    end

    puts "#{msg.type_code.to_s.rjust(2)} #{msg.address} #{line} #{human}"
  rescue ArgumentError => e
    puts "Unknown parse error in #{line}"
    raise e
  end
end

s.close
