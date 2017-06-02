class HttpException < StandardError
    attr_reader :message,:action,:response,:request
    def initialize(message = nil, action = nil, http_obj)
      @message = message
      @action = action
      @response = http_obj.response
      @request = http_obj.request
    end
end
