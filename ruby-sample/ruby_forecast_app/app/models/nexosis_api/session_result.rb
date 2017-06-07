module NexosisApi
    class SessionResult
        def initialize(sessionHash)
           sessionHash.each do |k,v|
               if(k == "session")
                   instance_variable_set("@#{k}", NexosisApi::Session.new(v)) unless v.nil?
               elsif k == "metrics"
                   instance_variable_set("@#{k}", NexosisApi::ImpactMetric.new(v)) unless v.nil?
               else
                   instance_variable_set("@#{k}", v) unless v.nil?
               end
            end
        end

        attr_accessor :session
        attr_accessor :metrics
        attr_accessor :data
    end
end
