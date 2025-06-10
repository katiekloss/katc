require 'socket'
require 'adsb'

s = TCPSocket.new ENV['DUMP1090_HOST'], 30002

while line = s.gets
  line = line[1..-3]
  begin
    msg = ADSB::Message.new(line)
    next unless msg.respond_to?(:type_code)

    human = case
    when msg.respond_to?(:identification)
      "ident #{msg.identification}"
    when msg.respond_to?(:latitude)
      "position #{msg.latitude} #{msg.longitude}"
    when msg.respond_to?(:heading)
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
