job "katc" {
  group "contact_writer" {
    task "run" {
      driver = "raw_exec"

      config {
        command = "/usr/bin/bundle"
        args = ["exec", "ruby", "contact_writer.rb"]
        work_dir = "${NOMAD_ALLOC_DIR}/katc/"
      }

      env {
        BUNDLE_PATH = "${NOMAD_ALLOC_DIR}/tmp/bundle"
      }

      template {
        data = <<EOH
{{ with nomadVar "nomad/jobs/katc" }}
RABBITMQ_URL={{ .rabbitmq_url }}
PG_URL={{ .pg_url }}
{{ end -}}
        EOH
        destination = "secrets/secrets.env"
        env = true
      }

      resources {
        cpu = 50
        memory = 100
      }
    }

    task "download" {
      lifecycle {
        hook = "prestart"
      }

      driver = "raw_exec"
      
      config {
        command = "/usr/bin/bundle"
        args = ["install"]
        work_dir = "${NOMAD_ALLOC_DIR}/katc"
      }

      env {
        BUNDLE_PATH = "${NOMAD_ALLOC_DIR}/tmp/bundle"
      }

      artifact {
        source = "https://code.kat5.dev/katie/katc/archive/main.zip"
        destination = "${NOMAD_ALLOC_DIR}"
      }

      resources {
        cpu = 100
        memory = 100
      }
    }
  }

  group "rabbit_writer" {
    constraint {
      attribute = "${attr.unique.hostname}"
      value = "tisiphone.hq.kat5.dev"
    }

    task "dump1090" {
      driver = "raw_exec"
      config {
        command = "/root/dump1090-fa/dump1090"
        args = ["--net", "--quiet"]
      }

      resources {
        cpu = 300
      }
    }

    task "download" {
      lifecycle {
        hook = "prestart"
      }

      driver = "raw_exec"

      config {
        command = "/usr/bin/bundle"
        args = ["install"]
        work_dir = "${NOMAD_ALLOC_DIR}/katc"
      }

      env {
        BUNDLE_PATH = "${NOMAD_ALLOC_DIR}/tmp/bundle"
      }

      artifact {
        source = "https://code.kat5.dev/katie/katc/archive/main.zip"
        destination = "${NOMAD_ALLOC_DIR}"
      }
    }

    task "run" {
      driver = "raw_exec"

      config {
        command = "/usr/bin/bundle"
        args = ["exec", "ruby", "rabbit_writer.rb"]
        work_dir = "${NOMAD_ALLOC_DIR}/katc/"
      }

      env {
        BUNDLE_PATH = "${NOMAD_ALLOC_DIR}/tmp/bundle"
      }

      template {
        data = <<EOH
{{ with nomadVar "nomad/jobs/katc" }}
RABBITMQ_URL={{ .rabbitmq_url }}
DUMP1090_HOSTNAME=localhost
{{ end -}}
        EOH
        destination = "secrets/secrets.env"
        env = true
      }

      resources {
        cpu = 50
        memory = 100
      }
    }

  }
}
