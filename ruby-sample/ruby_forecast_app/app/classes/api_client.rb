
require 'httparty'
require 'csv'

class ApiClient
	include HTTParty
	base_uri 'https://ml.nexosis.com/api'
	def initialize(key)
		@apiKey = key
		@headers = {"api-key" => @apiKey, "content-type" => "application/json"}
		@options = {:headers => @headers, :format => :json}
	end
	
	def get_sessions()
		session_url = '/sessions'
		Rails.cache.fetch(session_url, expires_in: 1.minute) do
			response = self.class.get(session_url,@options)
			if(response.success?)
				sessions = []
				response.parsed_response["results"].each{|session|
					sessions << NexosisApi::Session.new(session)
				}
				sessions
			end
		end
	end

	def get_session(sessionId)
		session_url = "/sessions/#{sessionId}"
		Rails.cache.fetch(session_url, expires_in: 30.second) do
			response = self.class.get(session_url,@options)
			if(response.success?)
				NexosisApi::Session.new(response.parsed_response)
			end
		end
	end

	def get_session_results(sessionId, as_csv = false)
		session_result_url = "/sessions/#{sessionId}/results"
		
		Rails.cache.fetch(session_result_url) do
			if as_csv
				@headers["Accept"] = "text/csv"
			end
			response = self.class.get(session_result_url,@options)
			@headers.delete("Accept")

			if(response.success?)
				if(as_csv)
					response.body
				else
					NexosisApi::SessionResult.new(response.parsed_response)
				end
			end
		end
	end

	def get_account_balance
		session_url = '/sessions'
		Rails.cache.fetch(session_url + '_balance', expires_in: 2.minute) do
			response = self.class.get(session_url,@options)
			response.headers["nexosis-account-balance"]
		end
	end

	def get_datasets
		dataset_url = "/data"
		Rails.cache.fetch(dataset_url) do
			response = self.class.get(dataset_url, @options)
			if(response.success?)
				response.parsed_response["dataSets"]
			else 
				"Failure: #{response.parsed_response['message']}"
			end
		end
	end
	
	def get_dataset(dataSetName, page = 0, page_size = 30, start_date = nil, end_date = nil)
		dataset_query_url = "/data/#{dataSetName}"
		query = { 
			"startDate" => start_date.to_s,
			"endDate" => end_date.to_s,
			"page" => page,
			"pageSize" => page_size
		}
		response = self.class.get(dataset_query_url,:headers => @headers, :query => query)
		if(response.success?)
			Rails.cache.write(dataset_query_url + query.inspect, response.parsed_response["data"])
			response.parsed_response["data"]
		end
	end

	def create_session(dataset_name, target_column, start_date, end_date, is_estimate=false, event_name = nil, type = "forecast")
		session_url = "/sessions/#{type}"
		query = { 
			"dataSetName" => dataset_name,
			"targetColumn" => target_column,
			"startDate" => start_date.to_s,
			"endDate" => end_date.to_s,
			"isestimate" => is_estimate.to_s
		}
		if(event_name.nil? == false)
			query["eventName"] = event_name
		end
		response = self.class.post(session_url, :headers => @headers, :query => query)
		if(response.success?)
			Rails.cache.delete('/sessions')
			Rails.cache.fetch("/sessions/#{response.parsed_response["sessionId"]}", expires_in: 30.second) do
				NexosisApi::Session.new(response.parsed_response)
			end
			session_hash = {"session" => response.parsed_response}.merge(response.headers)
			NexosisApi::SessionResponse.new(session_hash)
		else
			raise HttpException.new("Unable to create new #{type} session","Create session for dataset #{dataset_name}",response)
		end
	end

	def upload_dataset_csv(file,dataset_name)
		headers = {"api-key" => @apiKey, "content-type" => "text/csv"}
		dataset_url = "/data/#{dataset_name}"
		Rails.cache.delete(dataset_url)
		Rails.cache.delete('/sessions_balance')
		Rails.cache.delete('/data')
		csv = CSV.read(file)
		rowCount = 0
		content = ""
		#reading into rows so that we know how many to send, but will send content as csv
		csv.each do |row|
			rowCount += 1
			content.concat(row.map {|str| "\"#{str}\""}.join(',')).concat("\r\n")
			if(rowCount % 5000 == 0 || rowCount == csv.length)
				response = self.class.put(dataset_url, {:headers => headers, :body => content})
				raise HttpException.new("Unable to upload csv file to server","upload dataset csv",response) if response.success? == false
				content = ""
			end
		end
	end

	def get_forecast_model(dataset_name,target_column)
		model_url = "/data/#{dataset_name}/forecast/model/#{target_column}"
		Rails.cache.fetch(model_url) do
		response = self.class.get(model_url,@options)
			if(response.success?)
				NexosisApi::DatasetModel.new(response.parsed_response)
			end
		end
	end
		private
	def verify_response?(http_response)
		#todo: more complex processing of response state
		true
	end 
end