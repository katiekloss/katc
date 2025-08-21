job "katc-test" {
  group "rabbit_writer" {
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
{{ with nomadVar "nomad/jobs/katc-test" }}
RABBITMQ_URL={{ .rabbitmq_url }}
DUMP1090_HOSTNAME={{ .dump1090_host }}
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
