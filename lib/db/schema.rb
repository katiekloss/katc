module KATC
  module Schema
    def self.schema
      [
        "
    CREATE TABLE schema
    (
        migration_id        INT PRIMARY KEY,
        migrated_at         timestamp(0) with time zone     NOT NULL
    );
",
        "
    CREATE TABLE contacts
    (
        address char(6)                         NOT NULL PRIMARY KEY,
        callsign varchar(10)                    NULL,
        last_seen timestamp(0) with time zone   NOT NULL
    );
",
        "
    ALTER TABLE contacts DROP COLUMN callsign;

    ALTER TABLE contacts RENAME TO vehicles;

    CREATE TABLE contacts
    (
        address     char(6)                         NOT NULL,
        started_at  timestamp(0) with time zone     NOT NULL,
        last_at     timestamp(0) with time zone     NOT NULL,
        callsign    varchar(10)                     NULL,

        CONSTRAINT pk_contacts PRIMARY KEY (address, started_at),
        CONSTRAINT fk_contacts_address FOREIGN KEY (address) REFERENCES vehicles(address)
    );

    CREATE TABLE contact_logs
    (
        address             char(6)                         NOT NULL,
        contact_started_at  timestamp(0) with time zone     NOT NULL,
        received_at         timestamp(0) with time zone     NOT NULL,
        line                varchar(32)                     NOT NULL,

        CONSTRAINT fk_contact_logs_address FOREIGN KEY (address, contact_started_at) REFERENCES contacts(address, started_at)
    );
"
      ]
    end

    def self.migrate(db)
      begin
        current_version = db.exec("SELECT MAX(migration_id)::int FROM schema").getvalue(0, 0) + 1
      rescue PG::UndefinedTable
        current_version = 0
      end

      schema.each_with_index do |migration, i|
        next unless i >= current_version

        db.transaction { |trx|
          trx.exec(migration)
          trx.exec_params("INSERT INTO schema VALUES ($1, current_timestamp)", [i])
          puts "Updated schema to version #{i}"
        }
      end
    end
  end
end
