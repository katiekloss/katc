# frozen_string_literal: true

require_relative 'lib/db/schema'
require 'bunny'
require 'pg'
require 'date'
require 'adsb'

db = PG.connect(ENV['PG_URL'])
db.type_map_for_results = PG::BasicTypeMapForResults.new(db)

KATC::Schema.migrate(db)

rmq = Bunny.new
rmq.start
rmqc = rmq.create_channel
q = rmqc.queue('contact_writer')
q.bind('mode_s')

begin
  q.subscribe(block: true) do |_info, _properties, body|
    on_line(body)
  end
rescue ArgumentError => e
  puts "Unknown parse error in #{line}: #{e}"
end

def on_line(body)
  msg = ADSB::Message.new(body)
  return unless msg.respond_to?(:type_code)

  started_at = upsert_vehicle_and_contact(msg)

  if msg.respond_to?(:identification)
    db.exec(
      'UPDATE contacts SET callsign = $1 WHERE address = $2 AND started_at = $3 AND callsign IS NULL',
      [msg.identification, msg.address, started_at]
    )
  end

end

def upsert_vehicle_and_contact(msg)
  db.exec("
    INSERT INTO vehicles (address, last_seen)
    VALUES ($1, current_timestamp)
    ON CONFLICT (address) DO UPDATE
    SET last_seen = current_timestamp
  ",
          [msg.address])

  last_contact = db.exec(
    'SELECT last_at, started_at FROM contacts WHERE address = $1 ORDER BY last_at DESC LIMIT 1',
    [msg.address]
  )

  if last_contact.cmd_tuples.positive? && Time.now.to_i - last_contact.getvalue(0, 0).to_i < 600
    started_at = last_contact.getvalue(0, 1)
    db.exec('UPDATE contacts SET last_at = current_timestamp WHERE address = $1 AND started_at = $2',
            [msg.address, started_at])
  else
    last_contact = db.exec("
      INSERT INTO contacts (address, started_at, last_at)
      VALUES ($1, current_timestamp, current_timestamp)
      RETURNING started_at",
                           [msg.address])
    started_at = last_contact.getvalue(0, 0)
  end

  db.exec("
    INSERT INTO contact_logs (address, contact_started_at, received_at, line)
    VALUES ($1, $2, current_timestamp, $3)",
          [msg.address, started_at, body])

  return started_at
end

s.close
