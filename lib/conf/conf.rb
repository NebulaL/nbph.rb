# frozen_string_literal: true

require 'yaml'

# CONFIG_PATH = File.expand_path('conf.yml', __dir__)
CONFIG_PATH = './config.yml'
CONFIG = YAML.safe_load File.read(CONFIG_PATH)

$renv = {}
