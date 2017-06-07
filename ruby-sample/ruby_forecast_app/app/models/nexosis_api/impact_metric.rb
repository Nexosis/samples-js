module NexosisApi
    class ImpactMetric
        def initialize(metricHash)
           metricHash.each do |k,v|
               instance_variable_set("@#{k}", v) unless v.nil?
           end
        end

        attr_accessor :pvalue
        attr_accessor :absoluteEffect
        attr_accessor :relativeEffect
    end
end