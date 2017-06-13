module NexosisApi
    class AlgorithmSelection
        def initialize(dataHash)
           dataHash.each do |k,v|
               if k == "champion"
                   instance_variable_set("@#{k}", NexosisApi::AlgorithmRun.new(v))
               elsif k == "contestants"
                    @contestantArray = Array.new
                    v.each {|c| @contestantArray << NexosisApi::AlgorithmRun.new(c)}
                    instance_variable_set("@#{k}", @contestantArray)
               else
                  instance_variable_set("@#{k}", v) unless v.nil?    
               end
           end
        end

        attr_accessor :date
        attr_accessor :sessionId
        attr_accessor :champion
        attr_accessor :contestants
    end
end
