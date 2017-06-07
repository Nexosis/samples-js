module NexosisApi
    class DataRow
        def initialize(dataHash)
           dataHash.each do |k,v|
               instance_variable_set("@#{k}", v) unless v.nil?
           end
        end

        attr_accessor :timestamp
        attr_accessor :values
    end
end
