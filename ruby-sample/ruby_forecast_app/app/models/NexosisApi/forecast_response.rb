module NexosisApi
    class ForecastResponse
        def initialize(forecast_hash)
            forecast_hash.each do |k,v|
				if(k == "session")
					instance_variable_set("@#{k}", NexosisApi::Session.new(v)) unless v.nil?
				elsif(k == "nexosis-request-cost")
					instance_variable_set("@cost", v[0]) unless v.nil?
                elsif(k == "nexosis-account-balance")
                    instance_variable_set("@account_balance", v[0]) unless v.nil?
				end
			end
        end

        attr_accessor :session
        attr_accessor :cost
        attr_accessor :account_balance
    end
end