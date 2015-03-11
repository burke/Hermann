module Hermann
  module Errors
    class GeneralError < StandardError; end

    # Error for connectivity problems with the Kafka brokers
    class ConnectivityError < GeneralError; end

    # For passing incorrect config and options to kafka
    class ConfigurationError < GeneralError; end
  end
end

