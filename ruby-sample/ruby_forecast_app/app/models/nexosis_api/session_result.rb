module NexosisApi
    class SessionResult
        def initialize(sessionHash)
           sessionHash.each do |k,v|
                if k == "metrics"
                   instance_variable_set("@#{k}", NexosisApi::ImpactMetric.new(v)) unless v.nil?
               else
                   instance_variable_set("@#{k}", v) unless v.nil?
               end
            end
             @session = NexosisApi::Session.new(sessionHash.reject {|k,v| k == "data" || k == "metrics"})
        end

        attr_accessor :session
        attr_accessor :metrics
        attr_accessor :data
    end
end
