require 'api_client'

class AccountController < ApplicationController
	def index
		params.permit(:uploaded)
		if(params["uploaded"])
			@uploadMessage = "Your dataset has been uploaded."
		end
		
		client = ApiClient.new(Rails.application.secrets.api_key)
		@account_balance = client.get_account_balance
		@datasets = client.get_datasets
		render
	end

	def sessions
		client = ApiClient.new(Rails.application.secrets.api_key)
		@sessions = client.get_sessions()
		render
	end

	def session_status
		params.require(:session_id)
		client = ApiClient.new(Rails.application.secrets.api_key)
		@session = client.get_session params["session_id"]
		render "session"
	end

	def upload
		uploaded_io = params["datafile"]
		dataset = params["dataset_name"]
		File.open(Rails.root.join('public', 'uploads', uploaded_io.original_filename), 'wb') do |file|
			file.write(uploaded_io.read)
		end
		client = ApiClient.new(Rails.application.secrets.api_key)
		begin
			client.upload_dataset_csv Rails.root.join('public', 'uploads', uploaded_io.original_filename), dataset
		rescue HttpException => http_error
			@error_message = http_error.message
			render
			return
		end
		redirect_to action: 'index', :uploaded => true
	end

	def forecast
		params.require(:dataset_name)
		@dataset_name = params["dataset_name"]
	end

	def create_forecast
		params.require(:dataset_name)
		params.require(:target_column)
		params.require(:start_date)
		params.require(:end_date)
		params.permit(:is_estimate)
		client = ApiClient.new(Rails.application.secrets.api_key)
		estimate_only = params["is_estimate"] == "on" ? true : false
		begin
			@forecast_result = client.create_session params["dataset_name"], params["target_column"], params["start_date"], params["end_date"], estimate_only
		rescue HttpException => http_error
			@error = http_error
		end
		render "forecast"
	end

	def impact
		params.require(:dataset_name)
		@dataset_name = params["dataset_name"]
	end
	
	def create_impact
		params.require(:dataset_name)
		params.require(:target_column)
		params.require(:start_date)
		params.require(:end_date)
		params.require(:event_name)
		params.permit(:is_estimate)
		client = ApiClient.new(Rails.application.secrets.api_key)
		estimate_only = params["is_estimate"] == "on" ? true : false
		@event_name = params["event_name"]
		begin
			@impact_result = client.create_session params["dataset_name"], params["target_column"], params["start_date"], params["end_date"], estimate_only , params["event_name"], "impact"
		rescue HttpException => http_error
			@error = http_error
		end
		render "impact"
	end
	
end

