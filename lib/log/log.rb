# frozen_string_literal: true

require "logging"
require "./lib/conf/conf"

Logging.color_scheme(
  "bright",
  levels: {
    info: :green,
    warn: :yellow,
    error: :red,
    fatal: %i[white on_red]
  },
  date: :blue,
  logger: :cyan
)

Logging.appenders.stdout(
  "stdout",
  layout:
    Logging.layouts.pattern(
      pattern: '[%d] %-5l %c: %m\n',
      date_pattern: "%Y-%m-%d %H:%M:%S",
      color_scheme: "bright"
    )
)

Logging.appenders.rolling_file "devel.log",
                               age: "daily",
                               layout: Logging.layouts.json

Logging.appenders.file "prod.log", layout: Logging.layouts.json

def get_logger(name)
  log = Logging.logger[name]
  log.add_appenders CONFIG["log"]["appenders"]
  log.level = CONFIG["log"]["level"].to_sym
  log
end
