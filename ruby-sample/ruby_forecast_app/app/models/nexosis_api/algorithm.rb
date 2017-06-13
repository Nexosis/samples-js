module NexosisApi
    class Algorithm
        def initialize(algoHash)
           algoHash.each do |k,v|
               instance_variable_set("@#{k}", v) unless v.nil?
           end
        end

        attr_accessor :name
        attr_accessor :description
    end
end