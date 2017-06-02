module NexosisApi
    class AlgorithmRun
        def initialize(runHash)
           runHash.each do |k,v|
               if k == "metrics"
                   @metricSet = Array.new
                   v.each{|m| @metricSet << NexosisApi::Metric.new(m) unless m.nil?}
                   instance_variable_set("@#{k}", @metricSet)
               elsif k == "links"
                   @linkSet = Array.new
                   v.each{|l| @linkSet << NexosisApi::Link.new(l) unless l.nil?}
                   instance_variable_set("@#{k}", @linkSet)
               else
                    instance_variable_set("@#{k}", NexosisApi::Algorithm.new(v)) unless v.nil?
               end
           end
        end

        attr_accessor :algorithm
        attr_accessor :metrics
        attr_accessor :links
    end
end
