module NexosisApi
    class DatasetModel
        def initialize(dataHash)
           dataHash.each do |k,v|
               if k == "championContest"
                    instance_variable_set("@#{k}", NexosisApi::AlgorithmSelection.new(v))
               else
                    instance_variable_set("@#{k}", v) unless v.nil?
               end
           end
        end

        attr_accessor :datasetName
        attr_accessor :targetColumn
        attr_accessor :championContest
    end
end
