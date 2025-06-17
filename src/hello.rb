require 'socket'
require 'adsb'
require 'pg'

s = TCPSocket.new ENV['DUMP1090_HOST'], 30002
db = PG.connect ENV['PG_URL']

db.exec <<-SQL
CREATE TABLE IF NOT EXISTS
contacts
(
  address char(6)                         NOT NULL PRIMARY KEY,
  callsign varchar(10)                    NULL,
  last_seen timestamp(0) with time zone   NOT NULL
);
SQL

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
    
    db.exec("
      INSERT INTO contacts (address, callsign, last_seen)
      VALUES ($1, $2, current_timestamp)
      ON CONFLICT (address) DO UPDATE
      SET callsign = COALESCE($2, contacts.callsign),
          last_seen = current_timestamp
    ",
    [msg.address, if msg.respond_to?(:identification) then msg.identification else nil end])

  rescue ArgumentError => e
    puts "Unknown parse error in #{line}"
    raise e
  end
end

s.close
