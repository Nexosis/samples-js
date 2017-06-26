class ApplicationController < ActionController::Base
  protect_from_forgery with: :exception

  before_action :initialize_client

  attr_accessor :api_client

  def initialize_client
    @api_client = NexosisApi.client(:api_key => Rails.application.secrets.api_key)
  end
end
