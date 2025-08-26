# frozen_string_literal: true

require 'bunny'
require 'adsb'
require 'json'

rmq = Bunny.new
rmq.start
rmqc = rmq.create_channel
q = rmqc.queue('track_follower', exclusive: true)
xch = rmqc.exchange('katc', type: 'topic')
q.bind(xch, routing_key: 'adsb')

last_msg = Hash.new

begin
  q.subscribe(block: true) do |_info, _properties, body|
    msg = ADSB::Message.new(body)
    next unless msg.respond_to?(:latitude)

    if !last_msg.key?(msg.address)
      # The library needs a full message object, not just the body,
      # because they track the time they were initialized and that gets used somewhere.
      # This is probably unnecessary.
      # 
      # also TODO: fix this memory leak
      last_msg[msg.address] = [msg, msg.parity]
      next
    end

    # Sometimes you get the same parity twice (maybe reception issues, or the transponder being weird)
    # so wait to get one of the opposite parity
    if msg.parity != last_msg[msg.address][1]
      if msg.parity == :even
        report = ADSB::CPR::Report.new(msg, last_msg[msg.address][0])
      else
        report = ADSB::CPR::Report.new(last_msg[msg.address][0], msg)
      end

      report = {address: msg.address, latitude: report.latitude, longitude: report.longitude}

      xch.publish(report.to_json, routing_key: "track_updated", content_type: "application/json")

      last_msg[msg.address] = [msg, msg.parity]
    end

  end
end
