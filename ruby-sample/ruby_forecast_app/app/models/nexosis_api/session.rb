module NexosisApi
	class Session
		def initialize(sessionHash)
			sessionHash.each do |k,v|
				if(k == "links")
					links = Array.new
					v.each do |l| links << NexosisApi::Link.new(l) end
					instance_variable_set("@#{k}", links) unless v.nil?
				else
					instance_variable_set("@#{k}", v) unless v.nil?
				end
			end
		end

		attr_accessor :sessionId
		attr_accessor :type
		attr_accessor :status
		attr_accessor :statusHistory
		attr_accessor :extraParameters
		attr_accessor :dataSetName
		attr_accessor :targetColumn
		attr_accessor :startDate
		attr_accessor :endDate
		attr_accessor :links
	end
end

