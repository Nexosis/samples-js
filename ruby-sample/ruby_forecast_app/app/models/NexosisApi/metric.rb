module NexosisApi
    class Metric
        def initialize(metricHash)
           metricHash.each do |k,v|
               instance_variable_set("@#{k}", v) unless v.nil?
           end
        end

        attr_accessor :name
        attr_accessor :code
        attr_accessor :value
    end
end