require 'test_helper'
require 'api_client'

class ApiTest < ActiveSupport::TestCase

    @@apikey = Rails.application.secrets.api_key

    test "client connects" do
        target = ApiClient.new(@@apikey)
        actual = target.get_sessions()
        assert_not_nil(actual)
    end    
    
    test "sessions returned" do
        target = ApiClient.new(@@apikey)
        actual = target.get_sessions()
        assert_not_nil(actual)
        assert(actual.count > 0)
    end    

    test "sessionid gets one session" do
        target = ApiClient.new(@@apikey)
        all_sessions = target.get_sessions()
        one_key = all_sessions.first
        actual = target.get_session(one_key.sessionId)
        assert_not_nil(actual)
        assert(actual.class.name == "NexosisApi::Session")
    end

    test "upload csv throws httpexcep" do
        target = ApiClient.new("BadAPiKey")
        fileName = Rails.root.join('public', 'uploads', 'Location A.csv')
        assert_raise HttpException do
            target.upload_dataset_csv fileName,"TestCsvFail"
        end
    end
    
    test "forecast request returns session" do
        target = ApiClient.new(@@apikey)
        actual = target.create_session("GeneratedJson10K", "Column_2", Date.parse("3/1/1958"), Date.parse("5/1/1958"))
        assert_not_nil actual
        assert_not_nil actual.session.sessionId
    end
    
end