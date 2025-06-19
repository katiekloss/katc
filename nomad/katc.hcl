job "katc" {
  group "contact_writer" {
    task "run" {
      driver = "raw_exec"
      config {
        command = "/usr/local/bin/bundle"
        args = ["exec", "ruby", "src/contact_writer.rb"]
        work_dir = "${NOMAD_ALLOC_DIR}/katc"
      }
      env {
        BUNDLE_PATH = "${NOMAD_ALLOC_DIR}/tmp/bundle"
        PATH = "${PATH}:/usr/local/bin"
      }

      template {
        data = <<EOH
{{ with nomadVar "nomad/jobs/katc" }}
DUMP1090_HOST={{ .dump1090_host }}
PG_URL={{ .pg_url }}
{{ end -}}
        EOH
        destination = "secrets/secrets.env"
        env = true
      }
    }

    task "download" {
      lifecycle {
        hook = "prestart"
      }

      driver = "raw_exec"
      
      config {
        command = "/usr/local/bin/bundle"
        args = ["install"]
        work_dir = "${NOMAD_ALLOC_DIR}/katc"
      }

      env {
        BUNDLE_PATH = "${NOMAD_ALLOC_DIR}/tmp/bundle"
        PATH = "${PATH}:/usr/local/bin"
      }

      artifact {
        source = "git::https://github.com/katiekloss/katc"
        destination = "${NOMAD_ALLOC_DIR}/katc"
      }
    }
  }
}
