# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "logstash/timestamp"
require "logstash/util"
require "logstash/plugin_mixins/aws_config"
require "stud/interval"

# Pull events from the Amazon Web Services CloudWatch API.
#
# CloudWatch provides various logs from Lamdba functions.
#
# To use this plugin, you *must* have an AWS account

class LogStash::Inputs::CloudWatchLogs < LogStash::Inputs::Base
  include LogStash::PluginMixins::AwsConfig

  config_name "cloudwatchlogs"

  # If undefined, LogStash will complain, even if codec is unused.
  default :codec, "json"

  # Set how frequently CloudWatch should be queried
  #
  # The default, `900`, means check every 15 minutes
  config :interval, :validate => :number, :default => (60 * 15)

  # Set the granularity of the returned datapoints.
  #
  # Must be at least 60 seconds and in multiples of 60.
  config :period, :validate => :number, :default => 60

  public
  def aws_service_endpoint(region)
    { region: region }
  end

  public
  def register
    require "aws-sdk"
    AWS.config(:logger => @logger)

    @cloudwatchlogs = AWS::CloudWatchLogs::Client.new(aws_options_hash)

  end # def register

  def run(queue)
    Stud.interval(@interval) do
      @logger.debug('Polling CloudWatch API')
      group_names.each do |group_name|
        stream_names(group_name).each do |stream_name|
          opts = options(group_name, stream_name)
          @cloudwatchlogs.get_log_events(opts).each do |log|
            event = LogStash::Event.new(LogStash::Util.stringify_symbols(log))
            event['@timestamp'] = LogStash::Timestamp.new(log[:timestamp])
            event['message'] = log[:message]
            event['ingestion_time'] = log[:ingestion_time]
            decorate(event)
            queue << event
          end
        end
      end
    end # loop
  end # def run

  private
  def options(group_name, stream_name)
    {
      log_group_name: group_name,
      log_stream_name: stream_name,
      start_time: (Time.now - @interval).iso8601,
      end_time: Time.now.iso8601
    }
  end

  private
  def group_names()
    names = []
    @cloudwatchlogs.describe_log_groups().each do |group|
      names.push(group[:log_group_name])
    end
    names
  end

  private
  def stream_names(group_name)
    opts = { log_group_name: group_name }
    names = []
    @cloudwatchlogs.describe_log_streams(opts).each do |stream|
      names.push(stream[:log_stream_name])
    end
    names
  end

end # class LogStash::Inputs::CloudWatchLogs
