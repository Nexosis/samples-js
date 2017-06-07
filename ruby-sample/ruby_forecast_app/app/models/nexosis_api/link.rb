module NexosisApi
    class Link
        def initialize(linkHash)
           linkHash.each do |k,v|
               instance_variable_set("@#{k}", v) unless v.nil?
           end
        end

        attr_accessor :rel
        attr_accessor :href
    end
end
